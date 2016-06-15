//
//  DebugQuery.swift
//  SwiftPostgres
//
//  Created by Sigurd Ljødal on 15.06.2016.
//
//

import SwiftPostgres

public class Q : Query {
    public let sql: String

    init(_ sql: String) {
        self.sql = sql
    }
}
