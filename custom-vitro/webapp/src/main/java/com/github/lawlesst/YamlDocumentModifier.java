package com.github.lawlesst;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import edu.cornell.mannlib.vitro.webapp.beans.Individual;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ContextModelAccess;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ModelAccess;
import edu.cornell.mannlib.vitro.webapp.modules.searchEngine.SearchInputDocument;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.RDFServiceUtils;
import edu.cornell.mannlib.vitro.webapp.searchindex.documentBuilding.DocumentModifier;
import edu.cornell.mannlib.vitro.webapp.utils.configuration.ContextModelsUser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * YAML driven config for indexing Vitro changes in Solr. See resources/index.yml.
 */
public class YamlDocumentModifier implements DocumentModifier, ContextModelsUser {

    private static final Log log = LogFactory.getLog(YamlDocumentModifier.class);
    private RDFService rdfService;

    public YamlDocumentModifier() {
        log.info("YamlDocumentModifier initialized");
    }

    public static String readConfig(  ) {
        URL qurl = Resources.getResource("indexing.yml");
        try {
            return Resources.toString(qurl, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public void setContextModels(ContextModelAccess models) {
        this.rdfService = models.getRDFService(ModelAccess.WhichService.CONTENT);
    }

    public void modifyDocument(Individual individual, SearchInputDocument doc) {
        String subject = individual.getURI();
        String config = readConfig();
        log.debug("YAML Solr config:" + config);
        YamlReader reader = new YamlReader(config);
        Object object = null;
        try {
            object = reader.read();
        } catch (YamlException e) {
            e.printStackTrace();
        }
        //Map with Yaml config
        Map map = (Map)object;
        String prefixes = (String)map.get("prefixes");
        ArrayList<HashMap> fields = (ArrayList)map.get("fields");
        for (HashMap field: fields) {
            //System.out.print(field);
            String fieldName = field.get("name").toString();
            String selectRq = field.get("select").toString();
            ArrayList<String> constructRq = (ArrayList)field.get("construct");
            ArrayList<HashMap> raw = getData(subject, selectRq, constructRq, prefixes);
            for (HashMap item : raw) {
                // If it's a single key value hashmap, don't store as json. Just store value with fieldname.
                if (item.size() == 1) {
                    String val = (String)item.values().toArray()[0];
                    doc.addField(fieldName, val);
                } else {
                    JSONObject jItem = new JSONObject(item);
                    if (jItem.length() > 0) {
                        doc.addField(fieldName, jItem.toString());
                        log.info("Completed Solr yaml indexing for JSON field: " + subject + " field " + fieldName + "for " + subject);
                    }
                }
            }
        }
    }

    private ArrayList<HashMap> getData(String subjectUri, String selectQuery, ArrayList<String> constructQueries, String prefixes) {
        ArrayList<HashMap> outRows = new ArrayList<HashMap>();
        Model tmpModel = ModelFactory.createDefaultModel();
        selectQuery = prefixes + selectQuery.replace("?subject", "<" + subjectUri + ">");
        if (constructQueries != null) {
            for (String queryString : constructQueries) {
                queryString = prefixes + queryString.replace("?subject", "<" + subjectUri + ">");
                log.debug("CONSTRUCT:\n" + queryString);
                try {
                    this.rdfService.sparqlConstructQuery(queryString, tmpModel);
                } catch (RDFServiceException e) {
                    log.warn(e);
                    return outRows;
                }
            }
            log.debug("SELECT from TMP model:\n" + selectQuery);
            outRows = getFromModel(selectQuery, tmpModel);
        } else {
            log.debug("SELECT from Store:\n" + selectQuery);
            outRows = getFromStore(selectQuery);
        }
        return outRows;
    }


    private ArrayList<HashMap> getFromModel(String selectQuery, Model tmpModel) {
        ArrayList<HashMap> outRows = new ArrayList<HashMap>();
        try {
            QueryExecution qexec = QueryExecutionFactory.create(selectQuery, tmpModel);
            try {
                ResultSet results = qexec.execSelect();
                while ( results.hasNext() ) {
                    HashMap<String,String> thisItem = new HashMap();
                    QuerySolution soln = results.nextSolution();
                    for (String var: results.getResultVars()) {
                        //verify value exits
                        RDFNode val = soln.get(var);
                        if (val == null) {
                            continue;
                        }
                        // convert literals to lexical value
                        // convert uris to strings
                        if (val.isLiteral()) {
                            thisItem.put(var, val.asLiteral().getValue().toString());
                        } else {
                            thisItem.put(var, val.asResource().getURI());
                        }
                    }
                    //outItem = new JSONObject(thisItem);
                    outRows.add(thisItem);
                }
            } finally {
                qexec.close();
            }
        } catch (QueryParseException e) {
            log.warn(e);
            return outRows;
        }
        return outRows;
    }

    private ArrayList<HashMap> getFromStore(String selectQuery) {
        final ArrayList<HashMap> outRows = new ArrayList<HashMap>();
        try {
            ResultSet result = RDFServiceUtils.sparqlSelectQuery(selectQuery, this.rdfService);
            while(result.hasNext()) {
                HashMap<String,String> thisItem = new HashMap();
                QuerySolution soln = result.nextSolution();
                Iterator iter = soln.varNames();
                while(iter.hasNext()) {
                    String name = (String)iter.next();
                    RDFNode val = soln.get(name);
                    if(val != null) {
                        if (val.isLiteral()) {
                            thisItem.put(name, val.asLiteral().getValue().toString());
                        } else {
                            thisItem.put(name, val.asResource().getURI());
                        }
                        outRows.add(thisItem);
                    }
                }
            }
        } catch (Throwable var10) {
            log.error(var10, var10);
        }
        return outRows;
    }

    public String toString() {
        return this.getClass().getSimpleName();
    }

    public void shutdown() {
    }
}
