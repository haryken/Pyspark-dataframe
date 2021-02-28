# Pyspark-dataframe

## PySpark - Tạo DataFrame với các ví dụ

Bạn có thể Tạo một PySpark DataFrame bằng cách sử dụng toDF()và createDataFrame()các phương pháp, cả hai hàm này đều lấy các chữ ký khác nhau để tạo DataFrame từ RDD, danh sách và DataFrame hiện có.

Bạn cũng có thể tạo PySpark DataFrame từ các nguồn dữ liệu như TXT, CSV, JSON, ORV, Avro, Parquet, định dạng XML bằng cách đọc từ hệ thống tệp HDFS, S3, DBFS, Azure Blob, v.v.
Cuối cùng, PySpark DataFrame cũng có thể được tạo bằng cách đọc dữ liệu từ Cơ sở dữ liệu RDBMS và Cơ sở dữ liệu NoSQL.

## Tạo DataFrame từ RDD

Một cách dễ dàng để tạo PySpark DataFrame là từ một RDD hiện có. đầu tiên, hãy tạo một Spark RDD từ một Danh sách bộ sưu tập bằng cách gọi hàm song song () từ SparkContext . Chúng tôi sẽ cần đối tượng rdd này cho tất cả các ví dụ của chúng tôi bên dưới.

![image](https://user-images.githubusercontent.com/64195026/109418018-af9bb680-79f8-11eb-9d4d-3c192cf5af63.png)

### Sử dụng hàm toDF ()

Phương thức toDF () của PySpark RDD được sử dụng để tạo DataFrame từ RDD hiện có. Vì RDD không có 

![image](https://user-images.githubusercontent.com/64195026/109418046-de199180-79f8-11eb-9fb7-dd9fcf221398.png)

Nếu bạn muốn cung cấp tên cột cho toDF() phương pháp sử dụng DataFrame với tên cột làm đối số như hình dưới đây.

![image](https://user-images.githubusercontent.com/64195026/109418058-f2f62500-79f8-11eb-86d4-497b8050b596.png)

Điều này tạo ra lược đồ của DataFrame với các tên cột.

![image](https://user-images.githubusercontent.com/64195026/109418065-fc7f8d00-79f8-11eb-9d1f-9b802e50f823.png)

Theo mặc định, kiểu dữ liệu của các cột này suy ra kiểu dữ liệu. Chúng ta có thể thay đổi hành vi này bằng cách cung cấp lược đồ , trong đó chúng ta có thể chỉ định tên cột, kiểu dữ liệu và giá trị có thể làm trống cho mỗi trường / cột.

### Sử dụng createDataFrame () từ SparkSession

Sử dụng createDataFrame () từ SparkSession là một cách khác để tạo và nó lấy đối tượng rdd làm đối số. và chuỗi với toDF () để chỉ định tên cho các cột.

![image](https://user-images.githubusercontent.com/64195026/109418086-13be7a80-79f9-11eb-811d-41b2039cabfd.png)

## Tạo DataFrame từ Bộ sưu tập danh sách

Trong phần này, chúng ta sẽ xem cách tạo PySpark DataFrame từ một danh sách. Những ví dụ này sẽ tương tự như những gì chúng ta đã thấy trong phần trên với RDD, nhưng chúng ta sử dụng đối tượng dữ liệu danh sách thay vì đối tượng “rdd” để tạo DataFrame.

### Sử dụng createDataFrame () từ SparkSession

Gọi createDataFrame()from SparkSession là một cách khác để tạo PySpark DataFrame, nó lấy một đối tượng danh sách làm đối số. và chuỗi với toDF()để chỉ định tên cho các cột.

![image](https://user-images.githubusercontent.com/64195026/109418126-4a949080-79f9-11eb-918b-28663713c49d.png)

### Sử dụng createDataFrame () với kiểu Hàng

createDataFrame()có một chữ ký khác trong PySpark lấy bộ sưu tập kiểu Hàng và lược đồ cho tên cột làm đối số. Để sử dụng điều này, trước tiên chúng ta cần chuyển đổi đối tượng “dữ liệu” từ danh sách sang danh sách Hàng.

![image](https://user-images.githubusercontent.com/64195026/109418134-584a1600-79f9-11eb-940a-7162fa2b8bc0.png)

### Tạo DataFrame bằng lược đồ

Nếu bạn muốn chỉ định tên cột cùng với kiểu dữ liệu của chúng, trước tiên bạn nên tạo lược đồ StructType và sau đó gán nó trong khi tạo DataFrame.

![image](https://user-images.githubusercontent.com/64195026/109418154-69932280-79f9-11eb-9563-57c973dd7b13.png)

Điều này dẫn đến sản lượng thấp hơn.

![image](https://user-images.githubusercontent.com/64195026/109418163-731c8a80-79f9-11eb-8e93-a210920e2c05.png)

##  Tạo DataFrame từ các nguồn Dữ liệu

Trong thời gian thực, hầu hết bạn tạo DataFrame từ các tệp nguồn dữ liệu như CSV, Văn bản, JSON, XML, v.v.
PySpark theo mặc định hỗ trợ nhiều định dạng dữ liệu mà không cần nhập bất kỳ thư viện nào và để tạo DataFrame, bạn cần sử dụng phương pháp thích hợp có sẵn trong DataFrameReaderlớp.

### Tạo DataFrame từ CSV

Sử dụng csv()phương thức của DataFrameReaderđối tượng để tạo DataFrame từ tệp CSV. bạn cũng có thể cung cấp các tùy chọn như dấu phân cách sẽ sử dụng, cho dù bạn đã trích dẫn dữ liệu, định dạng ngày tháng, lược đồ suy luận, v.v. Vui lòng tham khảo PySpark Read CSV thành DataFrame
 
 ![image](https://user-images.githubusercontent.com/64195026/109418193-a4955600-79f9-11eb-92c5-62b0a8590e24.png)

 
### Tạo từ tệp văn bản (TXT)

Tương tự, bạn cũng có thể tạo DataFrame bằng cách đọc từ tệp Văn bản, sử dụng text()phương thức của DataFrameReader để làm như vậy.

![image](https://user-images.githubusercontent.com/64195026/109418200-ac54fa80-79f9-11eb-80e4-a85bd8a9a4d9.png)


### Tạo từ tệp JSON

PySpark cũng được sử dụng để xử lý các tệp dữ liệu bán cấu trúc như định dạng JSON. bạn có thể sử dụng json()phương thức của DataFrameReader để đọc tệp JSON vào DataFrame. Dưới đây là một ví dụ đơn giản.

![image](https://user-images.githubusercontent.com/64195026/109418215-b676f900-79f9-11eb-89fc-bfa87a46f4fc.png)


 Tương tự, chúng ta có thể tạo DataFrame trong PySpark từ hầu hết các cơ sở dữ liệu quan hệ mà tôi chưa trình bày ở đây và tôi sẽ để bạn khám phá.
 
## Các nguồn khác (Avro, Parquet, ORC, Kafka)

Có thể tạo DataFrame bằng cách đọc các tệp Avro, Parquet, ORC, Binary và truy cập bảng Hive và HBase, đồng thời đọc dữ liệu từ Kafka mà tôi đã giải thích trong các bài viết dưới đây, tôi khuyên bạn nên đọc chúng khi có thời gian.




