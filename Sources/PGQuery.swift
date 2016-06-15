//
//  PGQuery.swift
//  SwiftPostgres
//
//  Created by Sigurd Ljødal on 15.06.2016.
//
//

struct PGQuery : Query {
    let sql: String

    init(_ sql: String) {
        self.sql = sql
    }
}
