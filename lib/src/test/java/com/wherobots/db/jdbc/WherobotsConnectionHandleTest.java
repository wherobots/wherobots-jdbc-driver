package com.wherobots.db.jdbc;

import com.wherobots.db.jdbc.internal.ExecutionResult;
import com.wherobots.db.jdbc.internal.Query;
import com.wherobots.db.jdbc.models.Event;
import com.wherobots.db.jdbc.models.QueryState;
import com.wherobots.db.jdbc.models.Store;
import com.wherobots.db.jdbc.models.StoreResult;
import com.wherobots.db.jdbc.session.WherobotsSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for WherobotsJdbcConnection.handle() with Mockito.
 * Verifies the empty store results fix and defensive null-results fix.
 */
@ExtendWith(MockitoExtension.class)
class WherobotsConnectionHandleTest {

    @Mock
    private WherobotsSession mockSession;

    private WherobotsJdbcConnection connection;
    private ConcurrentHashMap<String, Query> queries;

    @BeforeEach
    void setUp() throws Exception {
        // Mock the session to prevent the loop() thread from doing anything harmful.
        // Use lenient() because the daemon thread may or may not call these depending
        // on timing; Mockito strict mode would flag them as unnecessary otherwise.
        lenient().when(mockSession.isClosed()).thenReturn(true);
        lenient().when(mockSession.iterator()).thenReturn(java.util.Collections.emptyIterator());

        connection = new WherobotsJdbcConnection(mockSession, new Properties());

        // Access the private queries map via reflection
        Field queriesField = WherobotsJdbcConnection.class.getDeclaredField("queries");
        queriesField.setAccessible(true);
        queries = (ConcurrentHashMap<String, Query>) queriesField.get(connection);
    }

    private Method getHandleMethod() throws Exception {
        Method handle = WherobotsJdbcConnection.class.getDeclaredMethod("handle", Event.class);
        handle.setAccessible(true);
        return handle;
    }

    /**
     * Test: Store configured, query succeeded, result_uri is null (empty result set).
     * Expected: onExecutionResult called with ExecutionResult(null, null, null),
     *           NOT retrieveResults().
     */
    @Test
    void storeConfiguredEmptyResultsDoesNotCallRetrieveResults() throws Exception {
        String executionId = "test-exec-1";
        WherobotsStatement statement = spy(new WherobotsStatement(connection));
        statement.setStore(Store.forDownload());

        queries.put(executionId, new Query(executionId, "SELECT 1 LIMIT 0", statement, QueryState.running));

        // Simulate: state_updated with state=succeeded, result_uri=null
        Event.StateUpdatedEvent event = new Event.StateUpdatedEvent();
        event.kind = "state_updated";
        event.executionId = executionId;
        event.state = QueryState.succeeded;
        event.resultUri = null;  // Empty result set!
        event.size = null;

        getHandleMethod().invoke(connection, event);

        // Verify: onExecutionResult was called with all-null ExecutionResult
        ArgumentCaptor<ExecutionResult> captor = ArgumentCaptor.forClass(ExecutionResult.class);
        verify(statement).onExecutionResult(captor.capture());

        ExecutionResult result = captor.getValue();
        assertNull(result.result(), "Should not have Arrow data");
        assertNull(result.error(), "Should not have an error");
        assertNull(result.storeResult(), "Should not have a store result (nothing was stored)");

        // Verify: retrieveResults was NOT called
        verify(mockSession, never()).send(contains("retrieve_results"));
    }

    /**
     * Test: Store configured, query succeeded, result_uri is present (non-empty results).
     * Expected: onExecutionResult called with StoreResult.
     */
    @Test
    void storeConfiguredWithResultUriProducesStoreResult() throws Exception {
        String executionId = "test-exec-2";
        WherobotsStatement statement = spy(new WherobotsStatement(connection));
        statement.setStore(Store.forDownload());

        queries.put(executionId, new Query(executionId, "SELECT 1", statement, QueryState.running));

        Event.StateUpdatedEvent event = new Event.StateUpdatedEvent();
        event.kind = "state_updated";
        event.executionId = executionId;
        event.state = QueryState.succeeded;
        event.resultUri = "https://example.com/presigned-url";
        event.size = 42L;

        getHandleMethod().invoke(connection, event);

        ArgumentCaptor<ExecutionResult> captor = ArgumentCaptor.forClass(ExecutionResult.class);
        verify(statement).onExecutionResult(captor.capture());

        ExecutionResult result = captor.getValue();
        assertNull(result.result());
        assertNull(result.error());
        assertNotNull(result.storeResult());
        assertEquals("https://example.com/presigned-url", result.storeResult().resultUri());
        assertEquals(42L, result.storeResult().size());
    }

    /**
     * Test: No store configured, query succeeded, result_uri is null.
     * Expected: retrieveResults() is called (existing behavior preserved).
     */
    @Test
    void noStoreConfiguredCallsRetrieveResults() throws Exception {
        String executionId = "test-exec-3";
        WherobotsStatement statement = spy(new WherobotsStatement(connection));
        // No setStore() call — store is null

        queries.put(executionId, new Query(executionId, "SELECT 1", statement, QueryState.running));

        Event.StateUpdatedEvent event = new Event.StateUpdatedEvent();
        event.kind = "state_updated";
        event.executionId = executionId;
        event.state = QueryState.succeeded;
        event.resultUri = null;
        event.size = null;

        getHandleMethod().invoke(connection, event);

        // Verify: onExecutionResult was NOT called (we're waiting for retrieve_results response)
        verify(statement, never()).onExecutionResult(any());

        // Verify: retrieveResults sends a message to the session
        verify(mockSession).send(contains("retrieve_results"));
    }

    /**
     * Test: ExecutionResultEvent with results=null.
     * Expected: onExecutionResult called with all-null ExecutionResult (defensive fix).
     */
    @Test
    void nullResultsInExecutionResultEventUnblocksStatement() throws Exception {
        String executionId = "test-exec-4";
        WherobotsStatement statement = spy(new WherobotsStatement(connection));

        queries.put(executionId, new Query(executionId, "SELECT 1", statement, QueryState.succeeded));

        Event.ExecutionResultEvent event = new Event.ExecutionResultEvent();
        event.kind = "execution_result";
        event.executionId = executionId;
        event.state = QueryState.succeeded;
        event.results = null;

        getHandleMethod().invoke(connection, event);

        ArgumentCaptor<ExecutionResult> captor = ArgumentCaptor.forClass(ExecutionResult.class);
        verify(statement).onExecutionResult(captor.capture());

        ExecutionResult result = captor.getValue();
        assertNull(result.result());
        assertNull(result.error());
        assertNull(result.storeResult());
    }
}
