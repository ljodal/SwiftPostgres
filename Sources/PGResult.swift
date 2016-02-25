import CLibpq

// Byte order swapping
import CoreFoundation

private func ntoh(int: Int16) -> Int16 {
    return Int16(bitPattern: CFSwapInt16BigToHost(UInt16(bitPattern: int)))
}

private func ntoh(int: Int32) -> Int32 {
    return Int32(bitPattern: CFSwapInt32BigToHost(UInt32(bitPattern: int)))
}

private func ntoh(int: Int64) -> Int64 {
    return Int64(bitPattern: CFSwapInt64BigToHost(UInt64(bitPattern: int)))
}

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

    public subscript(index: Int) -> String? {
        let value: RawData = self[index]

        switch (value) {
        case let .Text(data):
            return data
        case let .Binary(data, size):
            return String.fromCString(data)
        default:
            fatalError("Unable to decode value")
        }
    }

    public subscript(index: Int) -> Int? {
        let value: RawData = self[index]

        switch (value) {
        case .Nil:
            return nil
        case let .Text(data):
            return Int(data, radix: 10)
        case let .Binary(data, size):
            switch (size) {
            case 2:
                return Int(ntoh(UnsafeMutablePointer<Int16>(data)[0]))
            case 4:
                return Int(ntoh(UnsafeMutablePointer<Int32>(data)[0]))
            case 8:
                return Int(ntoh(UnsafeMutablePointer<Int64>(data)[0]))
            default:
                fatalError("Unsupported integer size: \(size)")
            }
        }
    }

    public subscript(index: Int) -> Int {
        let value: RawData = self[index]

        switch (value) {
        case let .Text(data):
            return Int(data, radix: 10)!
        case let .Binary(data, size):
            switch (size) {
            case 2:
                return Int(ntoh(UnsafeMutablePointer<Int16>(data)[0]))
            case 4:
                return Int(ntoh(UnsafeMutablePointer<Int32>(data)[0]))
            case 8:
                return Int(ntoh(UnsafeMutablePointer<Int64>(data)[0]))
            default:
                fatalError("Unsupported integer size: \(size)")
            }
        case .Nil:
            fatalError("Value is nil")
        }
    }

    public subscript(index: Int) -> RawData {

        let format = PQfformat(result.result, Int32(index))
        let value = PQgetvalue(result.result, Int32(self.index), Int32(index))
        let size = PQgetlength(result.result, Int32(self.index), Int32(index))
        let null = PQgetisnull(result.result, Int32(self.index), Int32(index))

        guard null == 0 else {
            return RawData.Nil()
        }

        switch (format) {
        case 0: // Text
            return RawData.Text(data: String.fromCString(value)!)
        case 1: // Binary
            return RawData.Binary(data: value, size: Int(size))
        default:
            fatalError("Unsupported data format")
        }
    }
}


public class PGResult {

    private let result: COpaquePointer

    public lazy var count: Int =  Int(PQntuples(self.result))
    public lazy var columnNames: [String] = self.getColumnNames()

    init(_ result: COpaquePointer) {
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