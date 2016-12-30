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
    "strings"
    "encoding/json"
    "net/http"
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
}

var siteId int
var siteVersion float64
var siteTitle string
var siteXmlParseUrl string
var db *sql.DB
var timeBefore time.Time
var err error
var newVendorsCount int
var newProductParamCount int
var newProductParamValueCount int
var ProductsCount int
const createdFormat = "2006-01-02 15:04:05"
var approvedParamsList []string
var scriptCount = 1
//var productSlice []Product
func main() {
    db, err = sql.Open("mysql", "root:121212@/raiment-shop.ru")
    defer db.Close()
    tmpFileName := "/tmp/tmpFile.xml"
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
        //"Тэг",
        "Страна дизайна",
        "Сезон",
        "Упаковка",
        "Материал",
        "Уход за изделием",
        //"Параметры изделия",
        "Материал подошвы",
        "Параметры модели",
        "Размер изделия на модели",
        "Цвет и обтяжка каблука",
        "Высота каблука",
        "Страна дизайна и производства",
        //"Размеры",
        "Вес",
        "Высота голенища",
    }
    if err2 := db.Ping(); err2 != nil {
        fmt.Println("Failed to keep connection alive")
    }
    sites, err := db.Query("SELECT id, version, title, xmlParseUrl FROM Site")
    defer sites.Close()
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    productChan := make(chan Product, 30000)
    go productParamsImport(productChan)
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
        newFile, err := os.Create(tmpFileName)
        if err != nil {
            log.Fatal(err)
        }
        defer newFile.Close()
        httpFile, err := http.Get(siteXmlParseUrl)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        numBytesWritten, err := io.Copy(newFile, httpFile.Body)
        if err != nil {
            log.Fatal(err)
        }
        timeLine("Размер файла " + strconv.FormatInt(numBytesWritten/1000000, 10) + " MB")
        httpFile.Body.Close()
        timeLine("Скачивание xml завершено")
        file, err := os.Open(tmpFileName)
        if err != nil {
            fmt.Println(err.Error())
            os.Exit(1)
        }

        timeLine("Импорт магазина " + siteTitle + " начат")
        newVendorsCount = 0
        ProductsCount, CategoriesCount, err := ImportSite(file, productChan)
        timeLine("Импорт магазина " + siteTitle + " завершен")
        timeLine("Обработано товаров: " + strconv.Itoa(ProductsCount))
        timeLine("Обработано категорий: " + strconv.Itoa(CategoriesCount))
        timeLine("Импортировано брендов: " + strconv.Itoa(newVendorsCount))
        timeLine("Импортировано параметров: " + strconv.Itoa(newProductParamCount))
        timeLine("Импортировано значений параметров: " + strconv.Itoa(newProductParamValueCount))
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        file.Close()
        updateVendorResults, err := db.Exec("UPDATE Site SET version=? WHERE id=?", siteVersion, siteId)
        checkErrorAndRollback(err)
        updateVendorResults.LastInsertId()
    }
    timeLine("Импорт магазинов завершен")
}


func ImportSite(reader io.Reader, productChan chan Product) (int, int, error) {
    d := xml.NewDecoder(reader)
    ProductsCount = 1;
    var (
        isProduct bool
        isCategory bool
        newParam string
        newProp string
        categoryName string
        categoryExternalId string
        categoryParentId string
    )
    Products := []Product{}
    Params := make(map[string]string)
    Attributes := make(map[string]string)
    Properties := make(map[string]string)
    pictures := []string{}
    CategoriesCount := 0
    for {
        t, tokenErr := d.Token()
        if tokenErr != nil {
            if len(productChan) == 0 {
                break
            }
        }
        switch t := t.(type) {
        case xml.StartElement:
            if t.Name.Local == "offer" {
                isProduct = true
                pictures = []string{}
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
                    if newProp == "picture" {
                        pictures = append(pictures, string(t.Copy()))
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
                if isProduct {
                    pics, err := json.Marshal(pictures)
                    checkErrorAndRollback(err)
                    Properties["picture"] = string(pics)
                    isProduct = false;
                }
                ProductsCount++
                product := Product{Params, Attributes, Properties}
                checkAndSaveProduct(product, productChan)
                Products = Products[:0]
                Params = make(map[string]string)
                Attributes = make(map[string]string)
                Properties = make(map[string]string)
            }
            if t.Name.Local == "category" {
                CategoriesCount++
                checkAndSaveCategories(Category{categoryName, categoryExternalId, categoryParentId})
                if isCategory {
                    isCategory = false;
                }
            }
        }
    }

    return ProductsCount, CategoriesCount, nil
}

func checkAndSaveProduct(Product Product, productChan chan<- Product) {
    var vendorId int64
    var productId int64
    oldProducts, err := db.Query("SELECT id FROM Product WHERE externalId=? AND siteId=?", Product.Attributes["id"], siteId)
    checkErrorAndRollback(err)
    for oldProducts.Next() {
        err = oldProducts.Scan(&productId)
        checkErrorAndRollback(err)
    }
    oldProducts.Close()
    //-------------------------------------- Vendor
    oldVendors, err := db.Query("SELECT id FROM Vendor WHERE name=?", Product.Properties["vendor"])
    checkErrorAndRollback(err)
    for oldVendors.Next() {
        err = oldVendors.Scan(&vendorId)
        checkErrorAndRollback(err)
    }
    oldVendors.Close()
    if vendorId > 0 {
        updateVendorResults, err := db.Exec("UPDATE Vendor SET version=? WHERE id=?", siteVersion, vendorId)
        checkErrorAndRollback(err)
        updateVendorResults.LastInsertId()
    } else {
        newVendorResults, err := db.Exec("INSERT INTO Vendor (name, version, siteId) VALUES(?, ?, ?)",
            Product.Properties["vendor"], strconv.FormatFloat(siteVersion, 'f', -1, 64), siteId)
        checkErrorAndRollback(err)
        vendorId, err = newVendorResults.LastInsertId()
        checkErrorAndRollback(err)
        newVendorsCount++
    }
    //---------------------------------------- Product
    var categoryId int64
    oldCategories, err := db.Query("SELECT id FROM ExternalCategory WHERE externalId=? AND siteId=?", Product.Properties["categoryId"], siteId)
    checkErrorAndRollback(err)
    for oldCategories.Next() {
        err = oldCategories.Scan(&categoryId)
        checkErrorAndRollback(err)
    }
    if categoryId > 0 {
        if productId > 0 {
            if Product.Properties["oldprice"] == "" {
                Product.Properties["oldprice"] = "0"
            }
            newProductResults, err := db.Exec("UPDATE Product SET " +
                    "version=?, currencyId=?, description=?, model=?, name=?, price=?, oldPrice=?, " +
                    "typePrefix=?, pictures=?, url=?, updated=?, vendorCode=?, categoryId=? " +
                    "WHERE id=?",
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
                Product.Properties["vendorCode"],
                categoryId,
                productId)
            checkErrorAndRollback(err)
            newProductResults.LastInsertId()
        } else {
            if Product.Properties["oldprice"] == "" {
                Product.Properties["oldprice"] = "0"
            }
            newProductResults, err := db.Exec("INSERT INTO Product (externalId, siteId, version, currencyId, description, model," +
                    " name, price, oldPrice, typePrefix, pictures, url, updated, vendorId, vendorCode, categoryId) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                Product.Properties["vendorCode"],
                categoryId)
            checkErrorAndRollback(err)
            productId, err = newProductResults.LastInsertId()
            checkErrorAndRollback(err)
        }
        Product.Properties["newId"] = strconv.FormatInt(productId, 10)
        productChan <- Product
    }
}

func productParamsImport (productChan <- chan Product) {
    //------------------------------------------------ ProductParams
    for Product := range productChan {
        for productParamKey, productParamValue := range Product.Params {
            if productParamKey == "Страна производства" {
                var firstIndex = strings.Index(productParamValue, "<")
                if firstIndex != -1 {
                    productParamValue = productParamValue[0:firstIndex]
                }
            }
            if productParamKey == "Страна дизайна" {
                var firstIndex = strings.Index(productParamValue, "<")
                if firstIndex != -1 {
                    productParamValue = productParamValue[0:firstIndex]
                }
            }
            if len(productParamValue) > 250 {
                productParamValue = productParamValue[0:250]
                lastIndex := strings.LastIndex(productParamValue, ", ")
                if lastIndex == -1 {
                    lastIndex = strings.LastIndex(productParamValue, "; ")
                }
                if lastIndex == -1 {
                    lastIndex = strings.LastIndex(productParamValue, "<br>")
                }
                if lastIndex == -1 {
                    lastIndex = strings.LastIndex(productParamValue, ". ")
                }
                if lastIndex == -1 {
                    lastIndex = strings.LastIndex(productParamValue, "· ")
                }
                if lastIndex == -1 {
                    lastIndex = 250
                }
                productParamValue = productParamValue[0:lastIndex]
            }
            if stringInSlice(productParamKey, approvedParamsList) {
                var dbProductParamId int64
                var dbProductParamValueId int64
                var productPropertyValueId int64
                dbProductParam, err := db.Query("SELECT id FROM ProductProperty WHERE name=?", productParamKey)
                checkErrorAndRollback(err)
                for dbProductParam.Next() {
                    err = dbProductParam.Scan(&dbProductParamId)
                    checkErrorAndRollback(err)
                }
                dbProductParam.Close()
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
                dbProductParamValue.Close()

                if dbProductParamValueId <= 0 {
                    newProductParamValueResults, err := db.Exec("INSERT INTO ProductPropertyValue (value, productPropertyId) VALUES(?, ?)", productParamValue, strconv.FormatInt(dbProductParamId, 10))
                    checkErrorAndRollback(err)
                    dbProductParamValueId, err = newProductParamValueResults.LastInsertId()
                    checkErrorAndRollback(err)
                    newProductParamValueCount++
                }
                dbProductParamValueLink, err := db.Query("SELECT productpropertyvalue_id FROM ProductPropertyValuesLink WHERE productpropertyvalue_id=? AND product_id=?", strconv.FormatInt(dbProductParamValueId, 10), Product.Properties["newId"])
                checkErrorAndRollback(err)
                for dbProductParamValueLink.Next() {
                    err = dbProductParamValueLink.Scan(&productPropertyValueId)
                    checkErrorAndRollback(err)
                }
                dbProductParamValueLink.Close()
                if productPropertyValueId <= 0 {
                    newProductParamValueLinkResults, err := db.Exec("INSERT INTO ProductPropertyValuesLink (productpropertyvalue_id, product_id) VALUES(?, ?)", strconv.FormatInt(dbProductParamValueId, 10), Product.Properties["newId"])
                    checkErrorAndRollback(err)
                    newProductParamValueLinkResults.LastInsertId()
                }
            }
        }
        if len(productChan) > 0 {
            if (len(productChan) / 5000) + 3 >= scriptCount {
                scriptCount++
                go productParamsImport(productChan)
            }
            fmt.Printf("\r%s", "--- Значений в буфере: " + strconv.Itoa(len(productChan)) +
                    " | Процессов: " + strconv.Itoa(scriptCount) + " | Товаров обработано: " + strconv.Itoa(ProductsCount) + " ---")
        }
    }
}
func checkAndSaveCategories(Category Category)  {
    var categoryId int64
    oldCategories, err := db.Query("SELECT id FROM ExternalCategory WHERE externalId=? AND siteId=?", Category.ExternalId, siteId)
    checkErrorAndRollback(err)
    for oldCategories.Next() {
        err = oldCategories.Scan(&categoryId)
        checkErrorAndRollback(err)
    }
    oldCategories.Close()
    if categoryId > 0 {
        updateCategoryResults, err := db.Exec("UPDATE ExternalCategory SET version=? WHERE id=?", siteVersion, categoryId)
        checkErrorAndRollback(err)
        updateCategoryResults.LastInsertId()
    } else {
        newCategoryResults, err := db.Exec("INSERT INTO ExternalCategory (externalId, parentId, name, version, siteId) VALUES(?, ?, ?, ?, ?)",
            Category.ExternalId, Category.ParentId, Category.Name, siteVersion, siteId)
        checkErrorAndRollback(err)
        newCategoryResults.LastInsertId()
    }
}
func timeLine(message string) {
    timeEnd := time.Now()
    fmt.Println("\n--- " + timeEnd.Format(createdFormat) + " ---")
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
        panic(err.Error())
    }
}