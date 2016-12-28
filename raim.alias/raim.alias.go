package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"strings"
)

var db *sql.DB
var err error
var filterAliases = make(map[string]string)
func main() {
	db, err = sql.Open("mysql", "root:1245655@/raiment-shop.ru")
	defer db.Close()
	if err2 := db.Ping(); err2 != nil {
		fmt.Println("Failed to keep connection alive")
	}
	//filterChan := make(chan map[string]string, 30000)

	// FilterAlias
	getFilterAliases()
	// Category
	getCategoryAliases()
	// Vendor
	getVendorAliases()

	fmt.Println(len(filterAliases))
	//filterChan <- FilterAlias{alias, aliasText}
	//for fc := range filterChan {
	//	fmt.Println(fc)
	//}
	checkError(err)

}

func checkError(err error) {
	if err != nil && err.Error() != "sql: no rows in result set" {
		panic(err.Error())
	}
}

func getFilterAliases () {
	filterAliasesDb, err := db.Query("SELECT alias, aliasText FROM FilterAlias")
	checkError(err)
	defer filterAliasesDb.Close()
	var alias string
	var aliasText string
	for filterAliasesDb.Next() {
		if err := filterAliasesDb.Scan(&alias, &aliasText); err != nil {
			checkError(err)
		}
		filterAliases[alias] = aliasText
	}
}

func getCategoryAliases () {
	categoryAliasesDb, err := db.Query("SELECT alias, name FROM Category")
	checkError(err)
	defer categoryAliasesDb.Close()
	var alias string
	var name string
	for categoryAliasesDb.Next() {
		if err := categoryAliasesDb.Scan(&alias, &name); err != nil {
			checkError(err)
		}
		if val, ok := filterAliases[alias]; ok {
		} else {
			if val == "" {
				filterAliases["category+" + alias] = name
			}
		}
	}
}

func getVendorAliases () {
	vendorAliasesDb, err := db.Query("SELECT alias, name FROM Vendor")
	checkError(err)
	defer vendorAliasesDb.Close()
	var alias string
	var name string
	var i int
	for vendorAliasesDb.Next() {
		if err := vendorAliasesDb.Scan(&alias, &name); err != nil {
			checkError(err)
		}
		for k, v := range filterAliases {
			var newAlias = k + "__" + "vendor+" + alias
			if val, ok := filterAliases[newAlias]; ok {
			} else {
				if !CaseInsensitiveContains(k, "vendor") {
					if val == "" {
						i++
						filterAliases[newAlias] = v + " " + name
						fmt.Printf("\r%s", i)
					}
				}
			}
		}
		//fmt.Printf("\r%s", len(filterAliases))
	}
}

func checkAlias (alias string) {
	firstLvl := strings.Split(alias, "__")
	for firstLvlVal := range firstLvl{
		secondLvl := strings.Split(firstLvlVal, "+")
		switch secondLvl {
		case "darwin":
		default:
		}
	}
}

func CaseInsensitiveContains(s, substr string) bool {
	s, substr = strings.ToUpper(s), strings.ToUpper(substr)
	return strings.Contains(s, substr)
}

