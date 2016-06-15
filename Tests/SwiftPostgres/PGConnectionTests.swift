import XCTest

//import CoreFoundation
@testable import SwiftPostgres

class ErrorHandling: XCTestCase {
    let connection = try! PGConnection(host: "localhost",
                                       port: 5432,
                                       database: "postgres",
                                       username: "postgres")

    func testSelectSmallInt() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT 1::smallint"), onSuccess: { result in
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

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testSelectInt() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT 1::int"), onSuccess: { result in
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

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testSelectString() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT 'test'::varchar"), onSuccess: { result in
            // Assert that we got 1 row with 1 column
            XCTAssertEqual(1, result.count, "Wrong number of rows")
            XCTAssertEqual(1, result.columns, "Wrong number of columns")

            // Assert that we got the expected value
            let value: String? = result[0][0]
            XCTAssertEqual("test", value, "Wrong value")

            // Fullfill the expectation
            queryExpectation.fulfill()

        }, onFailure: { error in
            XCTFail("Query failed: \(error)")
            queryExpectation.fulfill()
        })

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testSelectBigInt() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT 1::bigint"), onSuccess: { result in
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

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testSelectMultiple() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT * FROM generate_series(0, 10)"), onSuccess: { result in

            XCTAssertEqual(11, result.count, "Wrong number of rows")
            XCTAssertEqual(1, result.columns, "Wrong number of columns")

            var expected = 0
            for row in result {
                let a: Int = row[0]
                XCTAssertEqual(expected, a, "Wrong value")
                expected += 1
            }

            // Fullfill the expectation
            queryExpectation.fulfill()

        }, onFailure: { error in
            XCTFail("Query failed: \(error)")
            queryExpectation.fulfill()
        })

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testInvalidQuery() {
        let queryExpectation = expectation(withDescription: "Query execution completed")

        connection.execute(Q("SELECT abc"), onSuccess: { result in
            XCTFail("Query succeeded, expected failure")
            queryExpectation.fulfill()
        }, onFailure: { error in
            queryExpectation.fulfill()
        })

        waitForExpectations(withTimeout: 10) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
    }
}
