//
//  QueryExecutor.swift
//
//
//  Created by Sigurd Ljødal on 22.02.2016.
//
//

///
/// A protocol for general SQL queries.
///
public protocol Query {
    var sql: String { get }
}


public enum RawData {
    case binary(data: UnsafePointer<Int8>, size: Int)
    case text(data: String)
    case null()
}

///
/// One row of a result set
///
public protocol Row {

    /// Name of the columns
    var columns: [String] { get }

    /// Get the value at the column with the given name
    //subscript(name: String) -> T { get }
}

///
/// The result of executing a Query.
///
public protocol Result : Collection {

    associatedtype Element = Row
    associatedtype Index = Int

    /// Get the names of the columns contained in this result set
    var columnNames: [String] { get }
}


/// An executor capable of executing queries. The executor should
/// accept any reasonable amount of queries in parallel and execute
/// them as soon as possible. If parallel queries are not supported
/// by the underlaying database, queries should be queued and
/// executed in the order they are supplied.
public protocol QueryExecutor {

    // The type of result returned by this query executor
    associatedtype R = Result

    /// Start a new transaction (or savepoint)
    func begin(onSuccess: (Self) -> (), onFailure: (ErrorProtocol) -> ())

    /// Commit the current transaction or release the current savepoint
    func commit(onSuccess: (Self) -> (), onFailure: (ErrorProtocol) -> ())

    /// Roll back the current transaction or savepoint
    func rollback(onSuccess: (Self) -> (), onFailure: (ErrorProtocol) -> ())

    /// Execute the given query and call the callback methods when
    /// a result is ready
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    func execute(_ query: Query, onSuccess: (R) -> (), onFailure: (ErrorProtocol) -> ())

    /// Execute the given query and call the callback methods when
    /// each row of the result is ready.
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    // TODO
    //func execute(query: Query, onSuccess: (Row) -> (), onFailure: (ErrorType) -> ())
}
