import CLibpq

// Byte order swapping
import CoreFoundation

private func ntoh(oid: Oid) -> Oid {
    switch sizeof(Oid) {
    case 2:
        return Oid(ntoh(Int16(oid)))
    case 4:
        return Oid(ntoh(Int32(oid)))
    case 8:
        return Oid(ntoh(Int64(oid)))
    default:
        fatalError("Unsupported oid size: \(sizeof(Oid))")
    }
}

private func ntoh(int: Int16) -> Int16 {
    return Int16(bitPattern: CFSwapInt16BigToHost(UInt16(bitPattern: int)))
}

private func ntoh(int: Int32) -> Int32 {
    return Int32(bitPattern: CFSwapInt32BigToHost(UInt32(bitPattern: int)))
}

private func ntoh(int: Int64) -> Int64 {
    return Int64(bitPattern: CFSwapInt64BigToHost(UInt64(bitPattern: int)))
}

private func popcount(num: Int32) -> Int32 {
    var i = num
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

private func decodeArray(data: UnsafePointer<Int8>, _ size: Int) -> [Int?] {

    let dim = Int(ntoh(UnsafePointer<Int32>(data)[0]))
    let offset = Int(ntoh(UnsafePointer<Int32>(data.advancedBy(4))[0]))
    let oid = Int(ntoh(UnsafePointer<Oid>(data.advancedBy(8))[0]))
    let elems = Int(ntoh(UnsafePointer<Int32>(data.advancedBy(8 + sizeof(Oid)))[0]))
    let index = Int(ntoh(UnsafePointer<Int32>(data.advancedBy(12 + sizeof(Oid)))[0]))

    // TODO: Is the bitmap not included?
    //let nulls = ntoh(UnsafePointer<Int32>(data.advancedBy(16 + sizeof(Oid)))[0])
    //let items = UnsafePointer<Int32>(data.advancedBy(16 + sizeof(Oid) + offset * 4))


    print("Data size: \(size)")
    print("Dimmentions: \(dim)")
    print("Offset: \(offset)")
    print("Oid: \(oid)")
    print("Size: \(elems)")
    print("Index: \(index)")

    // Offset in bytes from start of data structure to the current element
    var i = 16 + sizeof(Oid)

    var result: [Int?] = []

    while i < size {
        let ptr = UnsafePointer<Int32>(data.advancedBy(i))
        let itemSize = ntoh(ptr[0])

        // If size if -1, this value is null
        if itemSize == -1 {
            result.append(nil)
            i += 4
        } else {
            result.append(Int(ntoh(ptr[1])))
            i += 4 + Int(itemSize)
        }
    }

    return result
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
                return Int(ntoh(UnsafePointer<Int16>(data)[0]))
            case 4:
                return Int(ntoh(UnsafePointer<Int32>(data)[0]))
            case 8:
                return Int(ntoh(UnsafePointer<Int64>(data)[0]))
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
                return Int(ntoh(UnsafePointer<Int16>(data)[0]))
            case 4:
                return Int(ntoh(UnsafePointer<Int32>(data)[0]))
            case 8:
                return Int(ntoh(UnsafePointer<Int64>(data)[0]))
            default:
                fatalError("Unsupported integer size: \(size)")
            }
        case .Nil:
            fatalError("Value is nil")
        }
    }

    public subscript(index: Int) -> [Int?] {
        let value: RawData = self[index]

        switch value {
        case let .Binary(data, size):
            return decodeArray(data, size)
        default:
            fatalError("Not binary")
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

    public lazy var columns: Int = Int(PQnfields(self.result))
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

    /// Get the PG Oid of the column at the given index
    public func oid(index: Int) throws -> Oid {

        guard 0..<columns ~= index else {
            throw PGError.Other(message: "Column \(index) is out of range: 0..<\(columns)")
        }

        return PQftype(self.result, Int32(index))
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
