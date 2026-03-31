from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, IntegerType
from pyspark.sql.functions import col

def get_review_schema():
    """Khuôn đúc chuẩn cho dữ liệu Ecommerce Reviews (Bản Full Enterprise)"""
    return StructType([
        # 1. Thông tin Review gốc
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("helpful", ArrayType(LongType()), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", DoubleType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", LongType(), True),
        StructField("reviewTime", StringType(), True),
        StructField("day_diff", IntegerType(), True),
        StructField("helpful_yes", IntegerType(), True),
        StructField("total_vote", IntegerType(), True),

        # 2. Thông tin Sản phẩm & Đơn hàng
        StructField("productName", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unitPrice", DoubleType(), True),
        StructField("orderID", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("totalAmount", DoubleType(), True),

        # 3. Thông tin Cửa hàng
        StructField("storeID", StringType(), True),
        StructField("storeName", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("storeType", StringType(), True),

        # 4. Thông tin Thanh toán
        StructField("paymentID", StringType(), True),
        StructField("paymentMethod", StringType(), True),
        StructField("paymentStatus", StringType(), True),

        # 5. Thông tin Giao hàng
        StructField("shippingID", StringType(), True),
        StructField("shippingMethod", StringType(), True),
        StructField("carrierName", StringType(), True),
        StructField("shippingStatus", StringType(), True)
    ])

def get_validation_rules():
    """Bộ quy tắc lọc dữ liệu lỗi (Data Quality Rules)"""
    return col("reviewerID").isNotNull() & col("asin").isNotNull() & col("overall").isNotNull()