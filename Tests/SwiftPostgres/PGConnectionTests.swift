import XCTest

//import CoreFoundation
@testable import SwiftPostgres

class ErrorHandling: XCTestCase {
    let connection = try! PGConnection(host: "localhost", port: 5432, database: "test")

    func testSimpleQuery() {
        let queryExpectation = expectationWithDescription("Query execution completed")

        connection.execute(Q("SELECT 1"), onSuccess: { result in
            // Assert that we got 1 row with 1 column
            XCTAssertEqual(1, result.count, "Wrong number of rows")
            XCTAssertEqual(1, result.columns, "Wrong number of columns")

            // Assert that we got the expected value
            let value: Int = result[0][0]
            XCTAssertEqual(1, value, "Wrong value")

            // Fullfill the expectation
            queryExpectation.fulfill()

        }, onFailure: { error in
            XCTFail("Query failed: \(error)")
            queryExpectation.fulfill()
        })

        waitForExpectationsWithTimeout(10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testInvalidQuery() {
        let queryExpectation = expectationWithDescription("Query execution completed")

        connection.execute(Q("SELECT abc"), onSuccess: { result in
            XCTFail("Query succeeded, expected failure")
            queryExpectation.fulfill()
        }, onFailure: { error in
            queryExpectation.fulfill()
        })

        waitForExpectationsWithTimeout(10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }
}
