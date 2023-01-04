package com.roche.mapping.publication;

import com.roche.mapping.ontology.MappingOntology;
import com.roche.mapping.tools.DomainVariable;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

@Slf4j
public abstract class AbstractFieldPublication extends OnlinePublication {

    protected Map<String, String> dictionary = HashMap.empty(); //field URI--> CODELIST URI
    //Pair<formOid,fieldOid> -> Pair<formUri,fieldUri>
    protected Map<Pair<String, String>, Pair<String, String>> formFieldUris = HashMap.empty();
    protected Map<String, List<Pair<String, Boolean>>> formUrisRepeatable = HashMap.empty(); //formOid -> formUri
    protected Map<String, String> formTherapeuticAreas = HashMap.empty(); //formUri -> therapeutic Area URI


    public abstract Option<String> getFixedUnit(String fieldUri);

    public abstract boolean isFieldFromID(String field);

    public abstract void load(CSVParser parser) throws NoSuchFieldException;

    public abstract List<String> search(DomainVariable field);

    public abstract List<String> search(String formOid, String fieldOid);

    public abstract Option<String> search(DomainVariable field, String sourceURI);

    //methods for the ones crf and non-crf have in common
    public Option<String> getCodelist(final String fieldUri) {
        return dictionary.get(fieldUri).filter(StringUtils::isNotEmpty);
    }

    public boolean isField(final String field) {
        return dictionary.containsKey(field);
    }

    public void load(final String urlString) throws IOException, NoSuchFieldException {
        URL url = new URL(urlString);
        CSVParser parser = CSVParser.parse(url, Charset.defaultCharset(), CSVFormat.RFC4180.withHeader());
        load(parser);
    }

    public void load(final File csvData) throws IOException, NoSuchFieldException {
        CSVParser parser = CSVParser.parse(csvData, Charset.defaultCharset(), CSVFormat.RFC4180.withHeader());
        load(parser);
    }

    public Option<Pair<String, String>> getFormFieldUri(final String formOid, final String fieldOid) {
        return formFieldUris.get(Pair.of(formOid, fieldOid));
    }

    /**
     * Search in the field dictionary if the given field URI exists
     * @param fieldURI The field URI
     * @return
     */
    public boolean isExistingFieldURI(final String fieldURI) {
        return dictionary.containsKey(MappingOntology.replacePrefixes(fieldURI));
    }

    /**
     * Search in the field dictionary if the given form URI exists
     * @param formURI The form URI
     * @return
     */
    public boolean isExistingFormURI(final String formURI) {
        return !formUrisRepeatable.values().filter(formList -> formList.contains(
                Pair.of(MappingOntology.replacePrefixes(formURI), true))
                || formList.contains(Pair.of(MappingOntology.replacePrefixes(formURI), false))).isEmpty();
    }

    public Option<List<Pair<String, Boolean>>> getFormUri(final String formOid, final String sourceFormUri) {
        Option<List<Pair<String, Boolean>>> formUris = formUrisRepeatable.get(formOid);
        if (formUris.isDefined() && formUris.get().size() > 1) {
            String ta = formTherapeuticAreas.getOrElse(sourceFormUri, "");
            formUris = Option.of(formUris.get()
                    .filter(formUri -> formTherapeuticAreas.getOrElse(formUri.getLeft(), "").equalsIgnoreCase(ta)));
            if (!formUris.isDefined() || formUris.get().isEmpty()) {
                formUris = formUrisRepeatable.get(formOid);
            }
        }
        return formUris;
    }

}
