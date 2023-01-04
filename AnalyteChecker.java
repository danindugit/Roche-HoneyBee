package com.roche.mapping.parsing.parser;

import com.roche.mapping.annotation.ParsingAuxiliaryInformationType;
import com.roche.mapping.annotation.Annotation;
import com.roche.mapping.dictionary.crf.CrfFieldDictionary;
import com.roche.mapping.parsing.ParseIssueType;
import com.roche.mapping.qc.QCCheckerExport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyteChecker {
    private final QCCheckerExport qcCheckerExport;
    private final CrfFieldDictionary crfFieldDictionary;

    public static final String FIELD_MSG = "The field %s has an analyte assigned, so 'local_lab' is expected in Parsing "
            + "(and couldn't be found in the annotation)";

    private static final String FORM_MSG = "The form %s has at least one analyte, so we are expecting that the annotation"
            + " for the date/time %s has 'local_lab' in Parsing.";

    public void checkAnalyte(final Annotation annotation) {
        if (annotation.getIsCrf()) {
            checkAnalyteSingleCase(annotation);
            checkAnalyteFullForm(annotation);
        }
    }
    private void checkAnalyteSingleCase(final Annotation annotation) {
        if (!crfFieldDictionary.getMeasurement(annotation.getFieldUri()).isEmpty()
                && !annotation.getParsingAuxiliaryInformation().containsKey(ParsingAuxiliaryInformationType.LOCAL_LAB)) {
            String oidMessage;
            //if the measurement uri is not empty and the parsing property does not have local lab, then create the QC warning
            //if an ep oid is present, display that, otherwise, display the lp oid in the warning
            if (!annotation.getEpFieldOid().isEmpty() && !annotation.getLpFieldOid().isEmpty()) {
                //if both ep and lp
                oidMessage = annotation.getEpFieldOid() + "|" + annotation.getLpFieldOid();
            } else if (!annotation.getEpFieldOid().isEmpty()) {
                //if only ep
                oidMessage = annotation.getEpFieldOid();
            } else {
                //if only lp
                oidMessage = annotation.getLpFieldOid();
            }
            qcCheckerExport.addMessage(annotation, ParseIssueType.WARNING, String.format(FIELD_MSG, oidMessage));
        }
    }

    private void checkAnalyteFullForm(final Annotation annotation) {
        //check analyte in the full form
        if (annotation.getAnnotation().equalsIgnoreCase("lb.lbdtc")
                && !annotation.getParsingAuxiliaryInformation().containsKey(ParsingAuxiliaryInformationType.LOCAL_LAB)
                && crfFieldDictionary.formHasAnalyte(annotation.getFormUri())) {
            qcCheckerExport.addMessage(annotation, ParseIssueType.WARNING, String.format(FORM_MSG, annotation.getFormOid(),
                    annotation.getAnnotation()));
        }
    }
}
