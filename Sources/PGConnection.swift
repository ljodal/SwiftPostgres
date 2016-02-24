
///
/// An implementation of a QueryExecutor that
/// executes queries againts PostgreSQL.
///

import CoreFoundation
import CLibpq

let queueLabel = "com.ljodal.pgconnection"

enum PGError: ErrorType {
    case ConnectionError(message: String)
    case Other(message: String)
}

/// This class represents a single connection to a database. One connection
/// can only execute a single query at a time, so if more than one query is
/// submited, they will be queued and executed in the order they are given.
class PGConnection: QueryExecutor {

    let source: dispatch_source_t
    let queue: dispatch_queue_t

    private let connection: COpaquePointer

    private var pgError: String? {
        return String.fromCString(PQerrorMessage(connection))
    }

    // The queue of queries to be executed
    //private let queries: [(query: Query, onSuccess: (Result) -> (), onError: (ErrorType) -> ())]

    init(host: String, port: UInt16, database: String) throws {

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

        // Try to send a query
        let status1 = PQsendQuery(connection, "SELECT * FROM test")
        guard status1 == 1 else {
            let msg = String.fromCString(PQerrorMessage(connection))
            throw PGError.Other(message: msg != nil ? msg! : "Failed to send query")
        }
    }


    /// Execute the given query and call the callback methods when
    /// a result is ready
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    func execute(query: Query, onSuccess: (Result) -> (), onFailure: (ErrorType) -> ()) {
    }

    /// Execute the given query and call the callback methods when
    /// each row of the result is ready.
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    func execute(query: Query, onSuccess: (Row) -> (), onFailure: (ErrorType) -> ()) {
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

        while true {
            // Check that enough data is available for us to read
            guard PQisBusy(connection) == 1 else {
                print("PQ is busy")
                return
            }


            let result = PQgetResult(connection)
            guard result != nil else {
                print("Null-result")
                return
            }

            let rows = PQntuples(result)


            print("Got a result: \(rows) rows")
        }

        // TODO: Check if we can read data from the postgresql connection

    }


    /// Make sure all resources held by this object are cleaned up
    deinit {

        print("Deinit called")

        dispatch_source_cancel(self.source)
    }
}
