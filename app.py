from pymongo import MongoClient
import bson
import pprint
import pandas as pd

pp = pprint.PrettyPrinter(indent=2)

ordersClient = MongoClient("mongodb://iaLstAtr:5KKZ94bhEHvunT5G@localhost:27017/ordersDB")
ordersDB = ordersClient["ordersDB"]
ordersCollection = ordersDB["orders"]

menusClient = MongoClient("mongodb://iaLstAtr:5KKZ94bhEHvunT5G@localhost:27018/menusDB")
menusDB = menusClient["menusDB"]
productsCollection = menusDB["products"]

storeId = "5e286736569f55001d548aa9"

allOrders = ordersCollection.find({ "storeId": storeId, "orderSellStatus": { "$lte" : 6, "$gte" : 4 }})
allProducts = productsCollection.find({ "storeId": storeId , "status": 1 })

print("All products: ")
productNames = []
productIds = []

for product in allProducts:
    productIds.append(str(product["_id"]))
    productNames.append(product["productName"])

pp.pprint(productNames)
pp.pprint(productIds)
print("Number of products : ", len(productNames))

# Create product.csv

productInfo = {
    "productId": productIds,
    "productName": productNames
}

product_df = pd.DataFrame(productInfo, columns = [ "productId", "productName" ])

product_df.to_csv(r"product.csv", index = False)

print(product_df)

count = 0

seatIds = []
productIds2 = []
ratings = []

for order in allOrders:
    for orderSeat in order["seats"]:
        seatId = str(orderSeat["_id"])
        for product in orderSeat["orderProducts"]:
            try:
                productIdx = productNames.index(product["productName"])
                productIds2.append(productIds[productIdx])
                seatIds.append(seatId)
                ratings.append(1)
            except:
                continue


print(len(seatIds))
print(len(productIds2))
print(len(ratings))

ratingData = {
    "seatId": seatIds,
    "productId": productIds2,
    "rating": ratings
}

ratings_df = pd.DataFrame(ratingData, columns = [ "seatId", "productId", "rating" ])

print(ratings_df)

ratings_df.to_csv(r"ratings.csv", index = False)

print(bson.ObjectId())
