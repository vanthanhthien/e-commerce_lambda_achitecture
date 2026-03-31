import boto3
import time
from pyspark.sql.functions import col, count, mean, when

class DynamoDBSink:
    def __init__(self, endpoint_url='http://localhost:8000', region_name='ap-southeast-1'):
        """Khởi tạo kết nối và đảm bảo bảng đã tồn tại trên DynamoDB"""
        try:
            self.dynamodb = boto3.resource(
                'dynamodb', 
                endpoint_url=endpoint_url, 
                region_name=region_name, 
                aws_access_key_id='dummy', 
                aws_secret_access_key='dummy'
            )
            self.table_name = 'realtime_metrics'
            self._ensure_table_exists()
            self.is_connected = True
        except Exception as e:
            print(f"⚠️ Không thể kết nối DynamoDB: {e}")
            self.is_connected = False

    def _ensure_table_exists(self):
        """Tự động tạo bảng nếu chưa có"""
        try:
            self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[{'AttributeName': 'batch_id', 'KeyType': 'S'}],
                AttributeDefinitions=[{'AttributeName': 'batch_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            time.sleep(1) # Chờ 1 giây để bảng tạo xong
            print(f"✅ Đã tạo thành công bảng {self.table_name} trên DynamoDB Local!")
        except Exception:
            # Bảng đã tồn tại thì bỏ qua
            pass

    def save_metrics(self, df, epoch_id):
        """Hàm tính toán siêu tốc trên RAM và bắn số tổng lên DynamoDB"""
        if not self.is_connected:
            return
            
        try:
            table = self.dynamodb.Table(self.table_name)
            
            # 1. Ép Spark tính toán số tổng cực nhanh
            metrics = df.select(
                count("*").alias("total_reviews"),
                mean("overall").alias("avg_rating"),
                count(when(col("overall") >= 4, True)).alias("positive_count"),
                count(when(col("overall") <= 2, True)).alias("negative_count")
            ).collect()[0]
            
            # 2. Bắn con số đã tính lên bảng
            table.put_item(
                Item={
                    'batch_id': str(epoch_id),
                    'timestamp': str(int(time.time())),
                    'total_reviews': int(metrics['total_reviews'] or 0),
                    'avg_rating': str(round(metrics['avg_rating'] or 0, 2)),
                    'positive_count': int(metrics['positive_count'] or 0),
                    'negative_count': int(metrics['negative_count'] or 0)
                }
            )
            print(f"⚡ [SPEED LAYER] Mẻ dữ liệu {epoch_id}: Đã bắn số liệu tổng hợp lên DynamoDB!")
        except Exception as e:
            print(f"⚠️ Lỗi khi ghi lên DynamoDB ở mẻ {epoch_id}: {e}")