import CLibpq


public class PGRow {
    private let result: PGResult
    private let index: Int

    public lazy var count: Int = Int(PQnfields(self.result.result))

    private init(_ result: PGResult, _ index: Int) {
        self.result = result
        self.index = index
    }
}

extension PGRow : Row {

    public var columns: [String] {
        return self.result.columnNames
    }

    public subscript(index: Int) -> Any? {
        return nil
    }

    public subscript(name: String) -> Any? {
        let index = name.withCString { str in
            PQfnumber(self.result.result, str)
        }

        guard index >= 0 else {
            fatalError("Unknown column: \(name)")
        }

        return nil
    }

    public func generate() -> IndexingGenerator<PGRow> {
        return IndexingGenerator(self)
    }

    public var startIndex: Int {
        return 0
    }

    public var endIndex: Int {
        return count
    }
}


public class PGResult {

    private let result: COpaquePointer

    public lazy var count: Int =  Int(PQntuples(self.result))
    public lazy var columnNames: [String] = self.getColumnNames()

    init(result: COpaquePointer) {
        self.result = result
    }

    /// Private helper function that loads column names, used
    /// to lazy load the column names array
    private func getColumnNames() -> [String] {
        return (0..<count).map { i in
            let name = String.fromCString(PQfname(result, Int32(i)))

            guard name != nil else {
                fatalError("Unable to get name for column \(i)")
            }

            return name!
        }
    }

    deinit {
        // Clean up postgres result
        // when object is deallocated
        PQclear(self.result)
    }

}


extension PGResult : Result {

    public subscript(index: Int) -> PGRow {
        guard index < count else {
            fatalError("Index out of bounds: \(index)")
        }

        return PGRow(self, index)
    }

    public func generate() -> IndexingGenerator<PGResult> {
        return IndexingGenerator(self)
    }

    public var startIndex: Int {
        return 0
    }

    public var endIndex: Int {
        return count
    }
}