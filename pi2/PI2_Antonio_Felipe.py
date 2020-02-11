import argparse
import csv
import re
from pyspark.sql import SparkSession
from pyspark.sql import Row


products_file_name = 'products.csv'
reviews_file_name = 'reviews.csv'


def do_csv(file_path):
    '''
    this function get the text file and struct in two csv files that will
    be load by spark in the next step
    '''
    products_file = open(products_file_name, 'w')
    products_writer = csv.writer(products_file)
    products_writer.writerow(['product_id', 'asin', 'group', 'salesrank'])

    reviews_file = open(reviews_file_name, 'w')
    reviews_writer = csv.writer(reviews_file)
    reviews_writer.writerow(['product_id', 'date', 'cutomer', 'rating', 'votes', 'helpful'])

    with open(file_path) as fp: 
        lines = fp.readlines()
        product_id = None
        asin = None
        title = None
        group = None
        salesrank = None
        for line in lines:
            if 'Id:' in line:
                # this is the first line of a product
                product_id = re.search(r"Id:(.*)\n", line).group(1).strip()
            elif 'ASIN:' in line:
                asin = re.search(r"ASIN:(.*)\n", line).group(1).strip()
            elif 'group:' in line:
                group = re.search(r"group:(.*)\n", line).group(1).strip()
            elif 'salesrank:' in line:
                salesrank = re.search(r"salesrank:(.*)\n", line).group(1).strip()
                product_dict = [product_id, asin, group, salesrank]
                products_writer.writerow(product_dict)
            elif 'cutomer:' in line:
                fields = [i for i in line.split(' ') if i != ' ' and i != '']
                review_dict = [product_id, fields[0], fields[2], fields[4], fields[6], re.sub('\s+', '', fields[8])]
                reviews_writer.writerow(review_dict)


def load_df(product):
    '''
    this function load the csv files and make queries
    '''
    spark = SparkSession.builder.appName("Amazon Meta").getOrCreate()

    # load the products data
    dfProducts = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').load(products_file_name)
    dfProducts.createOrReplaceTempView('product')

    # load the reviews data
    dfReviews = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').load(reviews_file_name)
    dfReviews.createOrReplaceTempView('review')

    # 1.a) Given a product, list the 5 most helpful and highest rated reviews
    sqlDF1 = spark.sql("SELECT * FROM review WHERE product_id=" + product + " ORDER BY helpful DESC, rating DESC LIMIT 5")
    print(sqlDF1.show())

    # 1.b) Given a product, list the 5 most helpful and lowest rated reviews
    sqlDF2 = spark.sql("SELECT * FROM review WHERE product_id=" + product + " ORDER BY helpful DESC, rating ASC LIMIT 5")
    print(sqlDF2.show())

    # 2) Given a product, list similar products with higher sales than it

    # 3) Given a product, show the daily evolution of the assessment averages over the time span covered in the input file

    group_list = dfProducts.select("group").distinct().collect()

    # 4) List the top 10 selling products in each product group
    for group in group_list:
        name = str(group.__getitem__("group"))
        sql_top_selling = spark.sql("SELECT product_id, salesrank FROM product WHERE group = '" + name + "' ORDER BY salesrank ASC LIMIT 10")
        sql_top_selling.show()

    # 5) List the 10 products with the highest average of positive useful reviews by product

    # 6) List the 5 product categories with the highest average of positive useful reviews per product


    # 7) List the 10 customers that most commented by product group
    for group in group_list:
        name = str(group.__getitem__("group"))
        sql_reviewers = spark.sql("SELECT cutomer, COUNT(cutomer) AS counter FROM review r LEFT JOIN product p on r.product_id = p.product_id WHERE p.group = '" + name + "' GROUP BY cutomer ORDER BY counter DESC LIMIT 10")
        sql_reviewers.show()

    spark.stop()


def main(file_path, product):
    do_csv(file_path)
    load_df(product)
    

if __name__ == "__main__":
    '''
    command example:
    /usr/local/spark/bin/spark-submit PI2_Antonio_Felipe.py --file='amazon-meta.txt' --product='7157'
    '''
    parser = argparse.ArgumentParser(description='A PySpark Script!')
    parser.add_argument("--file", required=True, type=str, help='File path')
    parser.add_argument("--product", required=False, type=str, help='Product Id')
    args = parser.parse_args()
    
    file_path = args.file
    product = args.product
    main(file_path, product)
