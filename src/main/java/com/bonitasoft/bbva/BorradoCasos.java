package com.bonitasoft.bbva;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.LoginAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.util.APITypeManager;

/**
 * @author Pablo Alonso de Linaje García
 */
public class BorradoCasos {
    private final Long processDefinitionId;
    private final Integer batchSize;
    private final Integer secondsWait;
    private final APISession apiSession;
    public BorradoCasos(String technicalUser, String technicalUserPassword, String bonitaUrl, String context, Long processDefinitionId, Integer batchSize, Integer secondsWait) {
//install install http://localhost:9100 bonita 6577895166825782270 5 1
        this.processDefinitionId = processDefinitionId;
        this.batchSize = batchSize;
        this.secondsWait = secondsWait;
        APISession apiSession = null;
        try {
            Map<String, String> settings = new HashMap<String, String>();
            settings.put("server.url", bonitaUrl);
            settings.put("application.name", context);
            APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, settings);
// get the LoginAPI using the TenantAPIAccessor
            LoginAPI loginAPI = TenantAPIAccessor.getLoginAPI();
// log in to the tenant to create a session
            apiSession = loginAPI.login(technicalUser, technicalUserPassword);

        } catch (Exception e) {

        }finally {
            this.apiSession = apiSession;
        }
    }

    public static void main(String[] args) throws Exception {
        BorradoCasos b = new BorradoCasos(args[0],args[1], args[2], args[3], Long.valueOf(args[4]), Integer.valueOf(args[5]) ,Integer.valueOf(args[6]) );
        b.borrarCasos();
    }
    public void borrarCasos() throws Exception {
        ProcessAPI pAPI = TenantAPIAccessor.getProcessAPI(apiSession);
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 0);
        sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
        Long numCases = pAPI.searchArchivedProcessInstances(sob.done()).getCount();
        System.out.println(System.currentTimeMillis());
        System.out.println("Hay un total de " + numCases + " archivados del proceso con id " + processDefinitionId);
        boolean continuar = true;
        Long latestId = 0L;
        while (continuar) {
            sob = new SearchOptionsBuilder(0, batchSize);
            sob.greaterThan(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, latestId);
            sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
            sob.sort(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, Order.ASC);
            SearchResult<ArchivedProcessInstance> result = pAPI.searchArchivedProcessInstances(sob.done());

            if (result.getCount() > 0) {
                List<Long> listCases = new ArrayList<Long>();
                for (ArchivedProcessInstance caso : result.getResult()) {
                    listCases.add(caso.getSourceObjectId());
                }
                latestId = listCases.get(listCases.size() - 1);
                pAPI.deleteArchivedProcessInstancesInAllStates(listCases);
                System.out.println("Se han borrados los casos hasta el id " + latestId);
                Thread.sleep(secondsWait*1000L);
            } else {
                continuar = false;
            }
        }
        System.out.println(System.currentTimeMillis());
    }
}
