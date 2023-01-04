package com.roche.mapping.parsing.parser;

import com.roche.mapping.annotation.AnnotationParsingType;
import com.roche.mapping.annotation.AnnotationType;
import com.roche.mapping.annotation.Annotation;
import com.roche.mapping.dictionary.GdsrDictionaries;
import com.roche.mapping.dictionary.common.Scenarios;
import com.roche.mapping.parsing.KnownParseException;
import com.roche.mapping.parsing.ParseIssueType;
import com.roche.mapping.parsing.Property;
import com.roche.mapping.qc.QCCheckerExport;
import com.roche.mapping.tools.DomainVariable;
import com.roche.mapping.tools.DomainVariableFactory;
import io.vavr.collection.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.roche.mapping.parsing.Property.HAS_ACTUAL_ELSE_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_ACTUAL_WHEN_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_DOUBLE_WHEN_OR_WHERE_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_ELSE_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_ELSE_IF_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_EMPTY_ANNOTATION;
import static com.roche.mapping.parsing.Property.HAS_GROUPBY;
import static com.roche.mapping.parsing.Property.HAS_IF_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_MERGE_TYPE;
import static com.roche.mapping.parsing.Property.HAS_MULTIPLE;
import static com.roche.mapping.parsing.Property.HAS_NONCRF_LAB;
import static com.roche.mapping.parsing.Property.HAS_NONCRF_PKC;
import static com.roche.mapping.parsing.Property.HAS_NONCRF_QRS;
import static com.roche.mapping.parsing.Property.HAS_NOTE;
import static com.roche.mapping.parsing.Property.HAS_NO_MAPPING;
import static com.roche.mapping.parsing.Property.HAS_NO_PARSE;
import static com.roche.mapping.parsing.Property.HAS_PAIRED_VARS;
import static com.roche.mapping.parsing.Property.HAS_PATNUM;
import static com.roche.mapping.parsing.Property.HAS_SIMPLE_EQUAL;
import static com.roche.mapping.parsing.Property.HAS_SIMPLE_WHEN_OR_WHERE_CONDITION;
import static com.roche.mapping.parsing.Property.HAS_SINGLE_WORD;
import static com.roche.mapping.parsing.Property.HAS_UNDUPLICATE;
import static com.roche.mapping.parsing.Property.NEED_USER_INPUT;
import static com.roche.mapping.parsing.Property.RECOGNIZED_CASE;
import static com.roche.mapping.parsing.Property.DTC_METHOD;
import static com.roche.mapping.parsing.Property.HAS_NSV_SPLIT;
import static com.roche.mapping.tools.StringHelper.getTextAfterFirst;
import static com.roche.mapping.tools.StringHelper.getTextAfterLast;
import static com.roche.mapping.tools.StringHelper.getTextBeforeFirst;
import static com.roche.mapping.tools.StringHelper.getTextBeforeLast;
import static com.roche.mapping.tools.StringHelper.getTextBetween;
import static com.roche.mapping.tools.StringHelper.splitByOperators;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.is;
import static java.lang.String.format;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnnotationCaseParser {
    private static final String PATNUM = "PATNUM";
    private static final String PART_OF = "part of ";
    private static final String USUBJID = ".USUBJID";
    private static final String IS_USER_DEFINED_ALGORITHM = "need_user_input";
    private static final String IS_DTC_METHOD = "dtc_method";
    private final DomainVariableFactory domainVariableFactory;
    private final QCCheckerExport qcCheckerExport;
    private final GdsrDictionaries dictionaries;
    private final AnalyteChecker qcAnalyteParser;

    public List<AnnotationCase> parseCase(final List<Annotation> annotations) {
        Annotation prevAnnotation = new Annotation();
        Annotation currAnnotation;
        boolean openIf = false;

        List<AnnotationCase> parsedCases = List.empty();

        List<AnnotationLine> parsedLines = List.empty();
        int annotationCounter = 0;
        for (Annotation annotation : annotations) {
            annotation.setMissingEndIf(false);
            try {
                AnnotationLine parsedLine = parse(annotation);
                currAnnotation = annotation;

                if (logEndIfError(openIf, prevAnnotation, currAnnotation)) {
                    prevAnnotation.setMissingEndIf(true);
                    parsedCases = parsedCases.append(new AnnotationCase(parsedLines));
                    parsedLines = List.empty();
                    openIf = false;
                }

                parsedLines = parsedLines.append(parsedLine);
                qcAnalyteParser.checkAnalyte(annotation);
                //check if we are entering a new if
                openIf = parsedLine.hasAnyProperty(HAS_IF_CONDITION, HAS_ELSE_IF_CONDITION, HAS_ELSE_CONDITION)
                        && !annotation.hasAuxiliarInfo("end if");
                //if we are outside of if condition or we have explicit end of if condition
                if (!openIf || annotation.hasAuxiliarInfo("end if")) {
                    parsedCases = parsedCases.append(new AnnotationCase(parsedLines));
                    parsedLines = List.empty();
                    openIf = false;
                }
                if (++annotationCounter == annotations.length()
                        && parsedLine.hasAnyProperty(HAS_IF_CONDITION, HAS_ELSE_IF_CONDITION, HAS_ELSE_CONDITION)
                        && !annotation.hasAuxiliarInfo("end if")) {
                    throw new KnownParseException("Not closed if statement in the last annotation");
                }
                prevAnnotation = annotation;
            } catch (KnownParseException e) {
                logGeneralError(e.getMessage(), annotation, ParseIssueType.ERROR);
            }

        }

        return parsedCases;
    }

    public AnnotationLine parse(final Annotation annotation) {
        AnnotationLine parsedLine = AnnotationLine.builder()
                .annotation(annotation)
                .hasFormAnnotation(hasFormAnnotation(annotation))
                .isCrf(annotation.getIsCrf())
                .build();

        if (annotation.hasAuxiliarInfo("unduplicate")) {
            parsedLine = handleUnduplicateAnnotationCase(parsedLine);
        }

        if (annotation.hasAuxiliarInfo("groupby")) {
            parsedLine = handleGroupByAnnotationCase(parsedLine);
        }

        if (isPatnumCase(annotation)) {
            parsedLine = addProperties(parsedLine, HAS_PATNUM);
        }

        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "multiple", HAS_MULTIPLE);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "nsv_split", HAS_NSV_SPLIT);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "paired", HAS_PAIRED_VARS);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "noncrf_pkc", HAS_NONCRF_PKC);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "noncrf_lab", HAS_NONCRF_LAB);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, "qrs", HAS_NONCRF_QRS);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, IS_USER_DEFINED_ALGORITHM, NEED_USER_INPUT);
        parsedLine = handleSimplePropertyFromAuxiliarInfo(parsedLine, IS_DTC_METHOD, DTC_METHOD);

        if (isEmptyCase(annotation)) {
            return addProperties(parsedLine, HAS_EMPTY_ANNOTATION);
        } else if (isNoParseCase(annotation)) {
            return handleNoParseCase(parsedLine);
        } else if (annotation.hasAuxiliarInfo("note")) {
            return handleNoteCase(parsedLine);
        } else if (isRelRecCase(annotation)) {
            return handleRelRecCase(parsedLine);
        }

        Pair<String, AnnotationLine> result = handleIfCases(parsedLine);

        String annotationText = result.getLeft();
        parsedLine = result.getRight();

        return handleMappingCases(annotationText, parsedLine);
    }

    private AnnotationLine handleSimplePropertyFromAuxiliarInfo(final AnnotationLine annotationLine,
                                                                final String expectedAuxiliarInfo,
                                                                final Property property) {
        if (annotationLine.getAnnotation().hasAuxiliarInfo(expectedAuxiliarInfo)) {
            return addProperties(annotationLine, property);
        } else {
            return annotationLine;
        }
    }

    protected boolean isPatnumCase(final Annotation annotation) {
        return !annotation.getIsCrf() && annotation.getAnnotation().toLowerCase().startsWith(PART_OF)
                && annotation.getAnnotation().endsWith(USUBJID)
                && (annotation.getLpFieldOid().equalsIgnoreCase(PATNUM) || annotation.getEpFieldOid().equalsIgnoreCase(PATNUM));
    }

    protected AnnotationLine handleMappingCases(final String annotationText,
                                                final AnnotationLine annotationLine) {
        if (isContactDsgCase(annotationText)) {
            return handleNoMappingCase(annotationLine, Scenarios.ScenarioCase._OTHER);
        } else if (isNotSubmittedCase(annotationText)) {
            return handleNoMappingCase(annotationLine, Scenarios.ScenarioCase.NOTSUBMITTED);
        } else {
            return handleIsMappingCase(annotationText, annotationLine);
        }
    }

    protected AnnotationLine handleIsMappingCase(final String annotationText,
                                                 final AnnotationLine annotationLine) {
        // Mapping exists
        AnnotationLine processedAnnotation = annotationLine;
        Annotation annotation = processedAnnotation.getAnnotation();

        // Check the presence of a merge condition
        if (Objects.nonNull(annotation.getConditionMerge()) && StringUtils.isNotEmpty(annotation.getConditionMerge().getType())) {
            processedAnnotation = addProperties(processedAnnotation, HAS_MERGE_TYPE).toBuilder()
                    .conditionMerge(annotation.getConditionMerge()).build();
        }

        if (annotationText.contains("=")) {
            return handleEqualCase(annotationText, processedAnnotation);
        } else {
            return handleSingleWordCase(annotationText, processedAnnotation);
        }
    }

    protected AnnotationLine handleEqualCase(final String annotationText,
                                             final AnnotationLine annotationLine) {
        return annotationText.toLowerCase().contains("when") || annotationText.toLowerCase().contains("where")
                ? handleWhenOrWhereCase(annotationText, annotationLine)
                : handleSimpleEqualCase(annotationText, annotationLine);
    }

    protected AnnotationLine handleWhenOrWhereCase(final String annotationText,
                                                   final AnnotationLine annotationLine) {
        AnnotationLine processedLine = annotationLine;
        // check if it's a real WHEN
        if (processedLine.getAnnotation().getAuxiliarInfo().toLowerCase().contains("when")) {
            processedLine = addProperties(processedLine, HAS_ACTUAL_WHEN_CONDITION);
        }

        String wh = annotationText.toLowerCase().contains("when") ? "when" : "where";

        // split in the three parts: singleWord when targetString = targetComparisonVariable
        String singleText = getTextBeforeFirst(annotationText, wh).trim(); // the A part or A = X
        String fullComparison = getTextAfterFirst(annotationText, wh).trim(); // the B=C part

        return singleText.contains("=")
                ? handleDoubleWhenOrWhereCase(singleText, fullComparison, processedLine)
                : handleSimpleWhenOrWhereCase(singleText, fullComparison, processedLine);
    }

    protected AnnotationLine handleSimpleWhenOrWhereCase(final String singleText, final String fullComparison,
                                                         final AnnotationLine annotationLine) {
        // set the single word, A, in A when B = C
        DomainVariable singleWordVariable =  domainVariableFactory.createSdtm(singleText); //get the domain and variable
        return addProperties(annotationLine, HAS_SIMPLE_WHEN_OR_WHERE_CONDITION, RECOGNIZED_CASE).toBuilder()
                        .singleWordVariable(singleWordVariable)
                        .comparisonComponents(setComparisonParts(fullComparison)) // set the comparison parts B and C
                        .scenarioCase(Scenarios.ScenarioCase.MAP_TOPIC)
                        .build();
    }

    protected AnnotationLine handleDoubleWhenOrWhereCase(final String singleText, final String fullComparison,
                                                         final AnnotationLine annotationLine) {
        return addProperties(annotationLine, HAS_DOUBLE_WHEN_OR_WHERE_CONDITION, RECOGNIZED_CASE).toBuilder()
                .comparisonComponents(setComparisonParts(singleText))
                .secondComparisonComponents(setComparisonParts(fullComparison))
                .scenarioCase(Scenarios.ScenarioCase.MAP_TOPIC)
                .build();
    }

    protected AnnotationLine handleSimpleEqualCase(final String annotationText,
                                                   final AnnotationLine annotationLine) {
        //simple = condition (no when or where)
        return addProperties(annotationLine, HAS_SIMPLE_EQUAL, RECOGNIZED_CASE).toBuilder()
                .comparisonComponents(setComparisonParts(annotationText)) // set the comparison parts A and B
                .build();
    }

    protected AnnotationLine handleSingleWordCase(final String annotationText,
                                                  final AnnotationLine annotationLine) {
        // try to recognize a single variable (potentially with prefix or suffix)
        DomainVariable singleWordVariable =  domainVariableFactory.createSdtm(annotationText);
        AnnotationLine processedLine = annotationLine.toBuilder()
                .singleWordVariable(singleWordVariable).build();
        if (!singleWordVariable.getDomain().isEmpty() || !singleWordVariable.getVariable().isEmpty()) {

            processedLine = addProperties(processedLine, HAS_SINGLE_WORD, RECOGNIZED_CASE);
        }
        return processedLine;
    }

    /**
     * Processes a comparison, e.g. X = A and Y = B or Z = D, and returns a list of components (X,A), (Y,B), (Z,C),
     * where the left components X, Y and Z are already recognized as variables
     * It also retrieves a list of operations (and, or) in order
     * @param annotationText
     * @return a Pair, with the left part being a List of pairs (DomainVariable,Value), and the right part being the
     * ordered operations (and, or) if exists
     * @throws KnownParseException
     */
    protected Pair<List<Pair<DomainVariable, String>>, List<String>> setComparisonParts(final String annotationText)
            throws KnownParseException {
        // split by "and" conditions, e.g. A=B and C=D
        Pair<List<String>, List<String>> splitParts = splitByOperators(annotationText, dictionaries.prefixSuffixVocabulary());
        List<String> textPart = splitParts.getLeft();
        List<String> logicalOperators = splitParts.getRight();

        AtomicReference<String> previousVariable = new AtomicReference<>("");
        // for each A=B part, we recognize and store the DomainVariable of A, and the text of B
        List<Pair<DomainVariable, String>> comparisonParts = List.ofAll(textPart.map(part -> {
            // split in the two parts: comparisonVariable = targetTextOrVariable
            String comparisonVariable = getTextBeforeFirst(part, "=").trim();
            String targetTextOrVariable = getTextAfterFirst(part, "=").trim();

            // If comparison variable is empty, reuse the previous one
            // This can happen for conditions like: A = B or C.
            if (comparisonVariable.isEmpty()) {
                comparisonVariable = previousVariable.get();
            }
            previousVariable.set(comparisonVariable);

            // check if the end of the targetTextOrVariable contains a suffix
            // if then it actually belongs to the comparison Variable, e.g. MHEVDTYP = 'SYMPTOMS' in SUPPMH
            String targetSuffix = dictionaries.prefixSuffixVocabulary().hasSuffix(targetTextOrVariable);
            // detect suffix and remove it if exists
            if (!targetSuffix.isEmpty()) {
                targetTextOrVariable = targetTextOrVariable.substring(0, targetTextOrVariable.length() - targetSuffix.length());
            }
            DomainVariable partVariable = domainVariableFactory.createSdtm(comparisonVariable, targetSuffix);
            return ImmutablePair.of(partVariable, targetTextOrVariable);
        }));
        return Pair.of(comparisonParts, logicalOperators);
    }

    protected boolean hasFormAnnotation(final Annotation annotation) {
        return Match(annotation.getAnnotationType()).of(
                Case($(is(AnnotationType.FORM)), true),
                Case($(is(AnnotationType.FIELD)), false)
        );
    }

    protected boolean isEmptyCase(final Annotation annotation) {
        return annotation.getAnnotation().isEmpty() && !isNoParseCase(annotation);
    }

    protected boolean isNoParseCase(final Annotation annotation) {
        return annotation.getAnnotationParsingType() == AnnotationParsingType.NO_PARSE;
    }

    protected boolean isRelRecCase(final Annotation annotation) {
        return annotation.getAnnotation().contains("via RELREC");
    }

    protected boolean isElseIfCase(final Annotation annotation) {
        return annotation.getAnnotation().toLowerCase().startsWith("else if ");
    }

    protected boolean isIfCase(final Annotation annotation) {
        return annotation.getAnnotation().toLowerCase().startsWith("if ");
    }

    protected boolean isElseCase(final Annotation annotation) {
        return annotation.getAnnotation().toLowerCase().startsWith("else ");
    }

    protected boolean isContactDsgCase(final String annotationText) {
        return annotationText.toUpperCase().startsWith("CONTACT DSG");
    }

    protected boolean isNotSubmittedCase(final String annotationText) {
        return annotationText.equalsIgnoreCase("NOT SUBMITTED");
    }

    /**
     * Add properties for the cases where the annotation is marked as No_parse
     * @param annotationLine
     * @return the annotationLine adding the property HAS_NO_PARSE and the scenario in the annotation
     */
    protected AnnotationLine handleNoParseCase(final AnnotationLine annotationLine) {
        try {
            return addProperties(annotationLine, HAS_NO_PARSE).toBuilder()
                    .scenarioCase(Scenarios.ScenarioCase
                            .parse(annotationLine.getAnnotation().getAlgorithms().getAlgorithm()))
                    .build();
        } catch (IllegalArgumentException e) {
            throw new KnownParseException(format("Unrecognized Algorithm %s in a No_parse case: %s",
                    annotationLine.getAnnotation().getAlgorithms().getAlgorithm(),
                    annotationLine.getAnnotation().getAnnotation()));
        }
    }

    /**
     * Add properties for the cases where the annotation contains unduplicate[] in the Aux. information
     * @param annotationLine
     * @return the annotationLine adding the property HAS_UNDUPLICATE and parsing the variables provided
     */
    protected AnnotationLine handleUnduplicateAnnotationCase(final AnnotationLine annotationLine) {
        Annotation annotation = annotationLine.getAnnotation();
        AnnotationLine parsedLine =
                addProperties(annotationLine, HAS_UNDUPLICATE).toBuilder()
                        .unduplicateVariables(getVariablesFromNoteAuxiliar(annotation.getAuxiliarInfo()))
                .build();

        if (!annotationLine.getHasFormAnnotation()) {
            throw new KnownParseException(format("Unduplicate only allowed in Form annotations: %s",
                    annotationLine.getAnnotation().getAnnotation()));
        }
        if (parsedLine.getUnduplicateVariables().isEmpty()) {
            throw new KnownParseException(format("Unduplicate withouth variables: %s",
                    annotationLine.getAnnotation().getAnnotation()));
        }
        return parsedLine;
    }

    /**
     * Add properties for the cases where the annotation contains groupby[] in the Aux. information
     * @param annotationLine
     * @return the annotationLine adding the property HAS_GROUPBY and parsing the variables provided
     */
    protected AnnotationLine handleGroupByAnnotationCase(final AnnotationLine annotationLine) {
        Annotation annotation = annotationLine.getAnnotation();
        AnnotationLine parsedLine =
                addProperties(annotationLine, HAS_GROUPBY).toBuilder()
                        .groupByVariables(getVariablesFromNoteAuxiliar(annotation.getAuxiliarInfo()))
                        .build();

        if (!annotationLine.getHasFormAnnotation()) {
            throw new KnownParseException(format("groupby only allowed in Form annotations: %s",
                    annotationLine.getAnnotation().getAnnotation()));
        }
        if (parsedLine.getGroupByVariables().isEmpty()) {
            throw new KnownParseException(format("groupby withouth variables: %s",
                    annotationLine.getAnnotation().getAnnotation()));
        }
        return parsedLine;
    }

    protected AnnotationLine handleNoteCase(final AnnotationLine annotationLine) {
        Annotation annotation = annotationLine.getAnnotation();
        AnnotationLine.AnnotationLineBuilder parsedLine =
                addProperties(annotationLine, HAS_NOTE).toBuilder()
                        .scenarioCase(Scenarios.ScenarioCase._OTHER)
                        .note(annotation.getAnnotation().trim());

        if (annotationLine.getHasFormAnnotation()) {
            parsedLine = parsedLine.domains(getVariablesFromNoteAuxiliar(annotation.getAuxiliarInfo()));
        } else {
            parsedLine = parsedLine
                    .noteVariables(parseSDTMVariableFromNoteAuxiliar(annotation.getAuxiliarInfo()));
        }
        return parsedLine.build();
    }

    protected AnnotationLine handleRelRecCase(final AnnotationLine annotationLine) {
        return annotationLine.toBuilder()
                .scenarioCase(Scenarios.ScenarioCase.RELREC)
                .domains(parseDomainFromRelRec(annotationLine.getAnnotation().getAnnotation()))
                .build();
    }

    protected Pair<String, AnnotationLine> handleIfCase(final AnnotationLine annotationLine,
                                                        final Property property, final String ifCondition) {
        String annotationText = annotationLine.getAnnotation().getAnnotation();
        if (annotationText.toLowerCase().contains(" then ")) {
            AnnotationLine parsedLine = addProperties(annotationLine, property).toBuilder()
                    .ifCondition(getTextBetween(annotationText, ifCondition, " then ").trim())
                    .build();
            //remove the first part from the annotation in order to continue the processing
            annotationText = getTextAfterFirst(annotationText, " then ");
            return ImmutablePair.of(annotationText, parsedLine);
        } else {
            throw new KnownParseException(format("Unrecognized '%s' case (missing 'then'?): %s", ifCondition, annotationText));
        }
    }

    protected Pair<String, AnnotationLine> handleElseCase(final AnnotationLine annotationLine) {
        Annotation annotation = annotationLine.getAnnotation();
        String annotationText = getTextAfterFirst(annotation.getAnnotation(), "else ");
        EnumSet<Property> properties = EnumSet.of(HAS_ELSE_CONDITION);

        // check if it's a real ELSE
        if (annotation.getAuxiliarInfo().toLowerCase().contains("else")) {
            properties.add(HAS_ACTUAL_ELSE_CONDITION);
        }

        return ImmutablePair.of(annotationText, addProperties(annotationLine, properties));
    }

    protected Pair<String, AnnotationLine> handleIfCases(final AnnotationLine annotationLine) {
        Annotation annotation = annotationLine.getAnnotation();
        if (isElseIfCase(annotation)) {
            return handleIfCase(annotationLine, HAS_ELSE_IF_CONDITION, "else if");
        } else if (isIfCase(annotation)) {
            return handleIfCase(annotationLine, HAS_IF_CONDITION, "if");
        } else if (isElseCase(annotation)) {
            return handleElseCase(annotationLine);
        } else {
            return ImmutablePair.of(annotation.getAnnotation(), annotationLine);
        }
    }

    protected AnnotationLine handleNoMappingCase(final AnnotationLine annotationLine,
                                                 final Scenarios.ScenarioCase scenarioCase) {
        return addProperties(annotationLine, HAS_NO_MAPPING, RECOGNIZED_CASE).toBuilder()
                .scenarioCase(scenarioCase)
                .build();
    }

    public static AnnotationLine addProperties(final AnnotationLine annotationLine,
                                               final Property... propertyList) {
        return addProperties(annotationLine, Arrays.asList(propertyList));
    }

    public static AnnotationLine addProperties(final AnnotationLine annotationLine,
                                               final Collection<Property> propertyList) {
        EnumSet<Property> properties = Objects.isNull(annotationLine.getAnnotationProperties())
                ? EnumSet.noneOf(Property.class) : annotationLine.getAnnotationProperties();
        properties.addAll(propertyList);
        return annotationLine.toBuilder().annotationProperties(properties).build();
    }

    protected List<DomainVariable> parseSDTMVariableFromNoteAuxiliar(final String auxiliarInfo) {
        if (auxiliarInfo.contains("[") && auxiliarInfo.contains("]")) {
            String text = getTextBetween(auxiliarInfo, "[", "]");
            // there could be multiple split by comma

            return text.isEmpty()
                    ? List.empty() : List.of(text.split(",")).distinct().map(String::trim).
                    map(domainVariableFactory::createSdtm);
        }
        return List.empty();
    }

    protected List<String> getVariablesFromNoteAuxiliar(final String auxiliarInfo) {
        if (auxiliarInfo.contains("[") && auxiliarInfo.contains("]")) {
            String variableText = getTextBetween(auxiliarInfo, "[", "]").trim();
            // there could be multiple split by comma
            return variableText.isEmpty()
                    ? List.empty() : List.of(variableText.split(",")).distinct().map(String::trim);
        }
        return List.empty();
    }

    protected List<String> parseDomainFromRelRec(final String annotationText) {
        // we make some assumptions
        // 1.- the first domain is in the first word, e.g. VS in "VS record linked to EX record via RELREC"
        // 2.- The second domain is before the last " record" work, e.g. EX in "VS record linked to EX record via RELREC"

        String processedAnnotationText = annotationText.trim();
        String firstDomain = getTextBeforeFirst(processedAnnotationText, " ");

        String lastString = getTextBeforeLast(processedAnnotationText, " record");
        String secondDomain = getTextAfterLast(lastString, " ");
        return List.of(firstDomain, secondDomain);
    }

    /**
     * Function that returns true if the annotation has an active if statement AND
     * we have switched to a field oid different from the previous annotation's one
     * @param prevAnnotation
     * @param currAnnotation
     * @return
     */
    private boolean isNewField(final Annotation prevAnnotation, final Annotation currAnnotation) {
        //if the current annotation has a new field uri, then return true
        return !(prevAnnotation.getFieldUri().equals(currAnnotation.getFieldUri()));
    }

    /**
     * Function that checks if we have the error in which we are missing an end if, and if so, logs the error
     * @param openIf
     * @param prevAnnotation
     * @param currAnnotation
     */
    boolean logEndIfError(final boolean openIf, final Annotation prevAnnotation,
                          final Annotation currAnnotation) {
        if (openIf && isNewField(prevAnnotation, currAnnotation)) {
            final String endIfError = "Missing 'end if' statement";
            logGeneralError(endIfError, prevAnnotation, ParseIssueType.WARNING);
            return true;
        }
        return false;
    }

    /**
     * logs an error to the qc checker
     * @param errorMessage
     * @param currAnnotation
     * @param issueType
     */
    private void logGeneralError(final String errorMessage, final Annotation currAnnotation,
                                 final ParseIssueType issueType) {
        String logMessage = format("    [Line %s] Parsing problem--> skipped. Reason: %s. Annotation: %s, STDM uri: %s",
                currAnnotation.getProvenanceLine() + 1, errorMessage,
                currAnnotation.getAnnotation(), currAnnotation.getStdmaUri());
        log.error(logMessage);
        qcCheckerExport.addMessage(currAnnotation, issueType, errorMessage);
    }
}
