
///
/// An implementation of a QueryExecutor that
/// executes queries againts PostgreSQL.
///

import CoreFoundation
import CLibpq
import Dispatch

let queueLabel = "com.ljodal.pgconnection"

public enum PGError: ErrorProtocol {
    case connectionError(message: String)
    case other(message: String)
}

/// This class represents a single connection to a database. One connection
/// can only execute a single query at a time, so if more than one query is
/// submited, they will be queued and executed in the order they are given.
public final class PGConnection: QueryExecutor {

    static let begin = PGQuery("BEGIN")
    static let commit = PGQuery("ROLLBACK")
    static let rollback = PGQuery("ROLLBACK")

    let source: DispatchSourceRead
    let queue: DispatchQueue

    private let connection: OpaquePointer

    // The queue of queries to be executed
    private var queries: [(query: Query, onSuccess: (PGResult) -> (), onFailure: (ErrorProtocol) -> ())] = []

    // If a query is currently being executed
    private var working = false

    /// Get the latest error message from libpq
    private var pgError: String? {
        // Get the postgres connection and return a String. This will copy
        // the string, so we do not need to worry about managing the memory
        return String(cString: PQerrorMessage(connection))
    }

    init(host: String, port: UInt16, database: String, username: String) throws {

        // Open a connection to the database
        // TODO: Do this asynchronously
        connection = PQconnectdb("postgresql://\(username)@\(host):\(port)/\(database)")

        // Make sure we are connectied
        guard PQstatus(connection) == CONNECTION_OK else {
            let msg = String(cString: PQerrorMessage(connection))
            throw PGError.connectionError(message: msg)
        }

        // Set the connection to non-blocking
        guard PQsetnonblocking(connection, 1) == 0 else {
            let msg = String(cString: PQerrorMessage(connection))
            throw PGError.other(message: msg)
        }

        // Get the underlaying socket for the postgres connection
        let fd = PQsocket(connection)
        guard fd >= 0 else {
            let msg = String(cString: PQerrorMessage(connection))
            throw PGError.other(message: msg)
        }

        self.queue = DispatchQueue(label: queueLabel, attributes: DispatchQueueAttributes.serial)
        self.source = DispatchSource.read(fileDescriptor: fd, queue: self.queue)

        // Set event handlers on the dispatch source
        self.source.setEventHandler(handler: self.handleEvent)
        self.source.setCancelHandler(handler: {

            // TODO: Clean up postgresql connection

            print("Cancel handler called")

        })
        self.source.resume()
    }

    public func begin(onSuccess: (PGConnection) -> (), onFailure: (ErrorProtocol) -> ()) {
        self.queue.async {
            self.queries.insert((query: PGConnection.begin, onSuccess: { result in
                onSuccess(self)
            }, onFailure: onFailure), at: 0)
            self.sendQuery()
        }
    }

    public func commit(onSuccess: (PGConnection) -> (), onFailure: (ErrorProtocol) -> ()) {
        self.queue.async {
            self.queries.insert((query: PGConnection.commit, onSuccess: { result in
                onSuccess(self)
            }, onFailure: onFailure), at: 0)
            self.sendQuery()
        }
    }

    public func rollback(onSuccess: (PGConnection) -> (), onFailure: (ErrorProtocol) -> ()) {
        self.queue.async {
            self.queries.insert((query: PGConnection.rollback, onSuccess: { result in
                onSuccess(self)
            }, onFailure: onFailure), at: 0)
            self.sendQuery()
        }
    }


    /// Execute the given query and call the callback methods when
    /// a result is ready
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    public func execute(_ query: Query, onSuccess: (PGResult) -> (), onFailure: (ErrorProtocol) -> ()) {

        // Add the query to the queue
        self.queue.async(execute: {
            self.queries.insert((query: query, onSuccess: onSuccess, onFailure: onFailure), at: 0)
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
        let msg = String(cString: PQerrorMessage(connection))
        q.onFailure(PGError.other(message: msg))

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
            let msg = String(cString: PQerrorMessage(connection))
            print(msg)
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
                _ = self.queries.popLast()
                self.working = false
                self.sendQuery()
                return
            }

            let status = PQresultStatus(result)
            guard status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK else {
                let msg = pgError
                queries.last!.onFailure(PGError.other(message: msg != nil ? msg! : "Query failed"))
                continue
            }

            self.queries.last!.onSuccess(PGResult(result!))
        }

        // TODO: Check if we can read data from the postgresql connection

    }


    /// Make sure all resources held by this object are cleaned up
    deinit {

        print("Deinit called")

        self.source.cancel()
    }
}
