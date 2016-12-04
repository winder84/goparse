package main

import (
    "encoding/xml"
    "fmt"
    "os"
    "io"
    "time"
    _ "github.com/go-sql-driver/mysql"
    "strconv"
    "database/sql"
    "log"
)

type Product struct {
    Params map[string]string
    Attributes map[string]string
    Properties map[string]string
}

type Category struct {
    Name string
    ExternalId string
    ParentId string
    //SiteId string
    //Version int
}

var siteId int
var siteVersion float64
var siteTitle string
var siteXmlParseUrl string
var db *sql.DB
var timeBefore time.Time
var err error
var tx *sql.Tx
var newVendorsCount int
var newProductParamCount int
var newProductParamValueCount int
const createdFormat = "2006-01-02 15:04:05"
var approvedParamsList []string
func main() {
    db, err = sql.Open("mysql", "root:07090530@/raiment-shop.ru")
    defer db.Close()
    tmpFileName := "tmpFile.xml"
    timeBefore = time.Now();
    timeLine("Импорт магазинов начат")
    approvedParamsList = []string{
        "Возраст",
        "Материал верха",
        "Цвет",
        "Страна производства",
        "Материал подкладки",
        "Пол",
        "Размер",
        "Артикул",
        "Материал стельки",
        "Тэг",
        "Страна дизайна",
        "Сезон",
        "Упаковка",
        "Материал",
        "Уход за изделием",
        "Параметры изделия",
        "Материал подошвы",
        "Параметры модели",
        "Размер изделия на модели",
        "Цвет и обтяжка каблука",
        "Высота каблука",
        "Особенности",
        "Страна дизайна и производства",
        "Размеры",
        "Вес",
        "Высота голенища",
    }
    if err2 := db.Ping(); err2 != nil {
        fmt.Println("Failed to keep connection alive")
    }
    sites, err := db.Query("SELECT id, version, title, xmlParseUrl FROM Site")
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    for sites.Next() {
        newProductParamCount = 0
        newProductParamValueCount = 0
        if err := sites.Scan(&siteId, &siteVersion, &siteTitle, &siteXmlParseUrl); err != nil {
            log.Fatal(err)
        }
        siteVersion = siteVersion + 0.01
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }

        timeLine("Скачивание xml начато")
        //newFile, err := os.Create(tmpFileName)
        //if err != nil {
        //    log.Fatal(err)
        //}
        //defer newFile.Close()
        //httpFile, err := http.Get(siteXmlParseUrl)
        //if err != nil {
        //    fmt.Println(err)
        //    os.Exit(1)
        //}
        //numBytesWritten, err := io.Copy(newFile, httpFile.Body)
        //if err != nil {
        //    log.Fatal(err)
        //}
        //timeLine("Размер файла " + numBytesWritten/1000000 + " MB")
        //httpFile.Body.Close()
        timeLine("Скачивание xml завершено")
        file, err := os.Open(tmpFileName)
        if err != nil {
            fmt.Println(err.Error())
            os.Exit(1)
        }

        timeLine("Импорт магазина " + siteTitle + " начат")
        newVendorsCount = 0
        ProductsCount, Categories, err := ImportSite(file)
        timeLine("Импорт магазина " + siteTitle + " завершен")
        timeLine("Обработано товаров: " + strconv.Itoa(ProductsCount))
        timeLine("Обработано категорий: " + strconv.Itoa(len(Categories)))
        timeLine("Импортировано брендов: " + strconv.Itoa(newVendorsCount))
        timeLine("Импортировано параметров: " + strconv.Itoa(newProductParamCount))
        timeLine("Импортировано значений параметров: " + strconv.Itoa(newProductParamValueCount))
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        file.Close()
    }
    timeLine("Импорт магазинов завершен")
}


func ImportSite(reader io.Reader) (int, map[int]Category, error) {
    d := xml.NewDecoder(reader)
    var (
        isProduct bool
        isCategory bool
        newParam string
        newProp string
        categoryName string
        categoryExternalId string
        categoryParentId string
    )
    Categories := make(map[int]Category)
    Params := make(map[string]string)
    Attributes := make(map[string]string)
    Properties := make(map[string]string)
    ProductsCount := 0
    j := 0
    for {
        t, tokenErr := d.Token()
        if tokenErr != nil {
            break
        }
        switch t := t.(type) {
        case xml.StartElement:
            if t.Name.Local == "offer" {
                isProduct = true
                for _, value := range t.Attr {
                    Attributes[string(value.Name.Local)] = value.Value
                }
            } else if t.Name.Local == "category" {
                isCategory = true
                for _, value := range t.Attr {
                    if string(value.Name.Local) == "id" {
                        categoryExternalId = string(value.Value)
                    }
                    if string(value.Name.Local) == "parentId" {
                        categoryParentId = string(value.Value)
                    }
                }
            } else {
                if isProduct {
                    if string(t.Name.Local) == "param" {
                        for _, value := range t.Attr {
                            if string(value.Name.Local) == "name" {
                                newParam = value.Value
                            }
                        }
                    } else {
                        newProp = string(t.Name.Local)
                    }
                }
            }
        case xml.CharData:
            if isProduct{
                if newParam != "" {
                    Params[newParam] = string(t.Copy())
                    newParam = ""
                }
                if newProp != "" {
                    if newProp == "picture" && len(Properties[newProp]) > 0 {
                        Properties[newProp] = Properties[newProp] + "," + string(t.Copy())
                    } else {
                        Properties[newProp] = string(t.Copy())
                    }
                    newProp = ""
                }
            }
            if isCategory {
                categoryName = string(t.Copy())
            }
        case xml.EndElement:
            if t.Name.Local == "offer" {
                ProductsCount++
                checkAndSaveProduct(Product{Params, Attributes, Properties})
                if isProduct {
                    isProduct = false;
                }
                Params = make(map[string]string)
                Attributes = make(map[string]string)
                Properties = make(map[string]string)
            }
            if t.Name.Local == "category" {
                j++
                Categories[j] = Category{categoryName, categoryExternalId, categoryParentId}
                if isCategory {
                    isCategory = false;
                }
            }
        }
    }

    return ProductsCount, Categories, nil
}

func checkAndSaveProduct(Product Product) {
    var vendorId int64
    var productId int64
    var oldVendorId int64
    tx, err = db.Begin()
    if err != nil {
        log.Fatal(err)
    }
    oldProducts := db.QueryRow("SELECT id FROM Product WHERE externalId=? AND siteId=?", Product.Attributes["id"], siteId)
    err = oldProducts.Scan(&productId)
    checkErrorAndRollback(err)
    if productId > 0 {
    } else {
        oldVendors := db.QueryRow("SELECT id FROM Vendor WHERE name=?", Product.Properties["vendor"])
        err = oldVendors.Scan(&oldVendorId)
        checkErrorAndRollback(err)
        if oldVendorId > 0 {
            vendorId = oldVendorId
        } else {
            newVendorResults, err := db.Exec("INSERT INTO Vendor (name, version, siteId) VALUES(?, ?, ?)",
                Product.Properties["vendor"], strconv.FormatFloat(siteVersion, 'f', -1, 64), siteId)
            checkErrorAndRollback(err)
            vendorId, err = newVendorResults.LastInsertId()
            checkErrorAndRollback(err)
            newVendorsCount++
        }
        if Product.Properties["oldprice"] == "" {
            Product.Properties["oldprice"] = "0"
        }
        newProductResults, err := db.Exec("INSERT INTO Product (externalId, siteId, version, currencyId, description, model, name, price, oldPrice, typePrefix, pictures, url, updated, vendorId, vendorCode) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                Product.Attributes["id"],
                siteId,
                strconv.FormatFloat(siteVersion, 'f', -1, 64),
                Product.Properties["currencyId"],
                Product.Properties["description"],
                Product.Properties["model"],
                Product.Properties["name"],
                Product.Properties["price"],
                Product.Properties["oldprice"],
                Product.Properties["typePrefix"],
                Product.Properties["picture"],
                Product.Properties["url"],
                time.Now().Format(createdFormat),
                strconv.FormatInt(vendorId, 10),
                Product.Properties["vendorCode"])
        checkErrorAndRollback(err)
        productId, err = newProductResults.LastInsertId()
        checkErrorAndRollback(err)
        for productParamKey, productParamValue := range Product.Params {
            if stringInSlice(productParamKey, approvedParamsList) {
                var dbProductParamId int64
                var dbProductParamValueId int64
                var productPropertyValueId int64
                dbProductParam := db.QueryRow("SELECT id FROM ProductProperty WHERE name=?", productParamKey)
                err = dbProductParam.Scan(&dbProductParamId)
                checkErrorAndRollback(err)
                if dbProductParamId <= 0 {
                    newProductParamResults, err := db.Exec("INSERT INTO ProductProperty (name) VALUES(?)", productParamKey)
                    checkErrorAndRollback(err)
                    dbProductParamId, err = newProductParamResults.LastInsertId()
                    checkErrorAndRollback(err)
                    newProductParamCount++
                }
                dbProductParamValue, err := db.Query("SELECT id FROM ProductPropertyValue WHERE value=? AND productPropertyId=?", productParamValue, strconv.FormatInt(dbProductParamId, 10))
                checkErrorAndRollback(err)
                for dbProductParamValue.Next() {
                    err = dbProductParamValue.Scan(&dbProductParamValueId)
                    checkErrorAndRollback(err)
                }

                if dbProductParamValueId <= 0 {
                    newProductParamValueResults, err := db.Exec("INSERT INTO ProductPropertyValue (value, productPropertyId) VALUES(?, ?)", productParamValue, strconv.FormatInt(dbProductParamId, 10))
                    checkErrorAndRollback(err)
                    dbProductParamValueId, err = newProductParamValueResults.LastInsertId()
                    checkErrorAndRollback(err)
                    newProductParamValueCount++
                }
                dbProductParamValueLink, err := db.Query("SELECT productpropertyvalue_id FROM ProductPropertyValuesLink WHERE productpropertyvalue_id=? AND product_id=?", strconv.FormatInt(dbProductParamValueId, 10), productId)
                checkErrorAndRollback(err)
                for dbProductParamValueLink.Next() {
                    err = dbProductParamValueLink.Scan(&productPropertyValueId)
                    checkErrorAndRollback(err)
                }
                if productPropertyValueId <= 0 {
                    newProductParamValueLinkResults, err := db.Exec("INSERT INTO ProductPropertyValuesLink (productpropertyvalue_id, product_id) VALUES(?, ?)", strconv.FormatInt(dbProductParamValueId, 10),strconv.FormatInt(productId, 10))
                    checkErrorAndRollback(err)

                    newProductParamValueLinkResults.LastInsertId()
                }
            }
        }
        oldVendors = nil
    }
    oldProducts = nil
}

func timeLine(message string) {
    timeEnd := time.Now()
    fmt.Println("--- " + timeEnd.Format(createdFormat) + " ---")
    timeLine := timeEnd.Unix() - timeBefore.Unix()
    if timeLine > 0 {
        fmt.Println("--- " + strconv.FormatInt(timeLine, 10) + " сек ---")
    }
    fmt.Println("--- " + message + " ---")
    timeBefore = timeEnd
}

func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func checkErrorAndRollback(err error) {
    if err != nil && err.Error() != "sql: no rows in result set" {
        tx.Rollback()
        timeLine("Транзакция отменена")
        panic(err.Error())
    }
}