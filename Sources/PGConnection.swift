
///
/// An implementation of a QueryExecutor that
/// executes queries againts PostgreSQL.
///

import CoreFoundation
import CLibpq

let queueLabel = "com.ljodal.pgconnection"

public enum PGError: ErrorType {
    case ConnectionError(message: String)
    case Other(message: String)
}

/// This class represents a single connection to a database. One connection
/// can only execute a single query at a time, so if more than one query is
/// submited, they will be queued and executed in the order they are given.
public class PGConnection: QueryExecutor {

    let source: dispatch_source_t
    let queue: dispatch_queue_t

    private let connection: COpaquePointer

    // The queue of queries to be executed
    private var queries: [(query: Query, onSuccess: (PGResult) -> (), onFailure: (ErrorType) -> ())] = []

    // If a query is currently being executed
    private var working = false

    /// Get the latest error message from libpq
    private var pgError: String? {
        // Get the postgres connection and return a String. This will copy
        // the string, so we do not need to worry about managing the memory
        return String.fromCString(PQerrorMessage(connection))
    }

    init(host: String, port: UInt16, database: String) throws {

        // Open a connection to the database
        // TODO: Do this asynchronously
        connection = PQconnectdb("postgresql://\(host):\(port)/\(database)")

        // Make sure we are connectied
        guard PQstatus(connection) == CONNECTION_OK else {
            let msg = String.fromCString(PQerrorMessage(connection))
            throw PGError.ConnectionError(message: msg != nil ? msg! : "Failed to connect")
        }

        // Set the connection to non-blocking
        guard PQsetnonblocking(connection, 1) == 0 else {
            let msg = String.fromCString(PQerrorMessage(connection))
            throw PGError.Other(message: msg != nil ? msg! : "Failed to set non-blocking mode")
        }

        // Get the underlaying socket for the postgres connection
        let fd = PQsocket(connection)
        guard fd >= 0 else {
            let msg = String.fromCString(PQerrorMessage(connection))
            throw PGError.Other(message: msg != nil ? msg! : "Failed to get socket")
        }

        self.queue = dispatch_queue_create(queueLabel, DISPATCH_QUEUE_SERIAL)
        self.source = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ, UInt(fd), 0, self.queue)

        // Set event handlers on the dispatch source
        dispatch_source_set_event_handler(self.source, self.handleEvent)
        dispatch_source_set_cancel_handler(self.source, {

            // TODO: Clean up postgresql connection

            print("Cancel handler called")

        })
        dispatch_resume(self.source)
    }


    /// Execute the given query and call the callback methods when
    /// a result is ready
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    public func execute(query: Query, onSuccess: (PGResult) -> (), onFailure: (ErrorType) -> ()) {

        // Add the query to the queue
        dispatch_async(self.queue, {
            self.queries.insert((query: query, onSuccess: onSuccess, onFailure: onFailure), atIndex: 0)
            self.sendQuery()
        })

    }

    private func sendQuery() {

        // Only send query if we are not currently executing a query
        guard !self.working else {
            print("Currently executing a query")
            return
        }

        // Ensure that a query is queued
        guard let q = queries.last else {
            print("No queries to execute")
            return
        }

        print("Sending query")

        // Try to send a query
        let status = PQsendQueryParams(connection, q.query.sql, 0, nil, nil, nil, nil, 1)
        print("Sent query: \(status)")
        guard status != 1 else {
            self.working = true
            return
        }

        // Failed to start query, send error message
        let msg = String.fromCString(PQerrorMessage(connection))
        q.onFailure(PGError.Other(message: msg != nil ? msg! : "Failed to send query"))

        // Try next query
        self.sendQuery()
    }

    /// This function is called by GCD whenever we can read
    /// data from the unerlaying socket. This method will
    /// check if enough data is available to read an entire
    /// result from the socket.
    private func handleEvent() {

        print("Handle event")

        // Make sure all available data is consumed
        guard PQconsumeInput(connection) == 1 else {
            let msg = String.fromCString(PQerrorMessage(connection))
            if msg != nil {
                print(msg)
            } else {
                print("Consume input failed")
            }
            return
        }


        // Check that enough data is available for us to read
        guard PQisBusy(connection) == 0 else {
            print("PQ is busy")
            return
        }

        while true {
            let result = PQgetResult(connection)
            guard result != nil else {
                print("Null-result")
                self.queries.popLast()
                self.working = false
                self.sendQuery()
                return
            }

            self.queries.last!.onSuccess(PGResult(result))
        }

        // TODO: Check if we can read data from the postgresql connection

    }


    /// Make sure all resources held by this object are cleaned up
    deinit {

        print("Deinit called")

        dispatch_source_cancel(self.source)
    }
}
