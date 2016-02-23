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
}

///
/// One row of a result set
///
public protocol Row {
}

///
/// The result of executing a Query.
///
public protocol Result {
    var rows: [Row] { get }
}


/// An executor capable of executing queries. The executor should
/// accept any reasonable amount of queries in parallel and execute
/// them as soon as possible. If parallel queries are not supported
/// by the underlaying database, queries should be queued and
/// executed in the order they are supplied.
public protocol QueryExecutor {

    /// Execute the given query and call the callback methods when
    /// a result is ready
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    func execute(query: Query, onSuccess: (Result) -> (), onFailure: (ErrorType) -> ())

    /// Execute the given query and call the callback methods when
    /// each row of the result is ready.
    ///
    /// If the executor is not able to process the query at this moment,
    /// the onFailure callback should be called immediately.
    func execute(query: Query, onSuccess: (Row) -> (), onFailure: (ErrorType) -> ())
}
