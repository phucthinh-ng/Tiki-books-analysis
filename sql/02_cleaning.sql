/*Project: Tiki Books Sales & Reviews Analysis (Vietnam E-commerce)
File: 02_cleaning_book_data.sql
Purpose: Clean and standardize book_data table dựa trên profiling insights
Key issues addressed:
- Duplicate product_id (16 rows) → deduplicate
- Missing/null vô lý (quantity null, price=0, authors "."/"..", publisher trống)
- Outliers cơ bản (rating=0)
- Scraped errors (publisher viết lộn xộn VN, category lẫn tên sách chỉ xuất hiện 1 lần)
Final output: VIEW book_data_clean (data sạch cơ bản, sẵn sàng JOIN comments hoặc enrichment sau)
*/

CREATE OR ALTER VIEW VW_Book_Data_Clean AS
-- Đặt lại tên cột và ép kiểu dữ liệu
WITH rename_and_cast_type AS (
    SELECT
        TRY_CAST(product_id AS BIGINT) AS tiki_product_id,
        CAST(title AS NVARCHAR(MAX)) AS title,
        CAST(authors AS NVARCHAR(MAX)) AS authors,
        CAST(category AS NVARCHAR(MAX)) AS category,
        CAST(manufacturer AS NVARCHAR(MAX)) AS publisher,
        CAST(cover_link AS NVARCHAR(MAX)) AS cover_link,
        TRY_CAST(original_price AS DECIMAL(18, 2)) AS original_price,
        TRY_CAST(current_price AS DECIMAL(18, 2)) AS current_price,
        TRY_CAST(quantity AS INT) AS sold_quantity,
        TRY_CAST(n_review AS INT) AS count_reviews,
        TRY_CAST(avg_rating AS NUMERIC(10, 2)) AS avg_rating,
        TRY_CAST(pages AS INT) AS book_pages
    FROM book_data
)
-- Xóa duplicate
, deduplicate_step AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY tiki_product_id
               ORDER BY (SELECT NULL)
           ) AS rn
    FROM rename_and_cast_type
)
-- Dữ liệu sau khi xóa duplicate
, filter_basic_step AS (
    SELECT * FROM deduplicate_step
    WHERE rn = 1
      AND tiki_product_id IS NOT NULL
)
-- Xóa các đơn có quantity bằng null vì vô lý
, remove_wrong_data AS (
SELECT *
FROM filter_basic_step
where sold_quantity IS NOT null
)
-- Xử lý các giá trị null ở các cột
, handle_null AS (
    SELECT
        tiki_product_id,
        title,
        CASE
            WHEN authors IN ('.', '..', '') THEN NULL
            ELSE authors
        END AS authors,
        COALESCE(NULLIF(CAST(publisher AS NVARCHAR(MAX)), ''), N'Unknown') AS publisher,
        cover_link,
        original_price,
        current_price,
        sold_quantity,
        count_reviews,
        NULLIF(avg_rating, 0) AS avg_rating,
        book_pages,
        category
    FROM remove_wrong_data
)
 
, clean_value AS (
    SELECT
        tiki_product_id,
        title,
        authors,
        CASE
            WHEN publisher LIKE N'%Kinh Tế Quốc Dân%' THEN N'NXB ÐH Kinh Tế Quốc Dân'
            WHEN publisher LIKE N'%Hồnng Ðức%' THEN N'NXB Hồng Đức'
            WHEN publisher LIKE N'%Dân Trí%' THEN N'NXB Dân Trí'
            WHEN publisher LIKE N'%Phụ Nữ%' THEN N'NXB Phụ Nữ'
            WHEN publisher LIKE N'%Hà Nội%' AND publisher NOT LIKE N'%Quốc Gia%' THEN N'NXB Hà Nội'
            WHEN publisher LIKE N'%TP.HCM%' OR publisher LIKE N'%TPHCM%' THEN N'NXB Tổng hợp TPHCM'
            ELSE REPLACE(publisher, N'Nhà Xuất Bản', N'NXB')
        END AS publisher,
        cover_link,
        original_price,
        current_price,
        sold_quantity,
        CASE
        -- Những trường hợp chỉ xuất hiện 1 lần thường không phải là category
            WHEN CAST(category AS NVARCHAR(400)) IN (
                SELECT CAST(category AS NVARCHAR(400))
                FROM handle_null
                GROUP BY CAST(category AS NVARCHAR(400))
                HAVING COUNT(*) = 1
            ) THEN NULL
            ELSE category
        END AS category,
        count_reviews,
        avg_rating,
        book_pages
    FROM handle_null
)
 
, last_cleaning AS (
	SELECT 
    tiki_product_id,
    title,
    COALESCE(authors, N'Unknown') AS authors,
    publisher,
    cover_link,
    original_price,
    current_price,
    sold_quantity,
    COALESCE(category, N'Unknown') AS category,
    count_reviews,
    avg_rating, 
    book_pages
FROM clean_value
)

SELECT
*
FROM last_cleaning

--SELECT * FROM VW_Book_Data_Clean

/* =============================================
   SUMMARY – BOOK DATA CLEANING
   - Deduplicated by product_id
   - Removed logically invalid records (quantity IS NULL)
   - Standardized publisher and category fields
   - Converted invalid numeric values to NULL
   - Output is analytics-ready for enrichment
   ============================================= */


/*
 * -----------
 * ----------- table comments
 */

-- Đổi tên
/* Bước 2: Clean & Deduplicate Comments Table */
CREATE OR ALTER VIEW VW_Comments_Clean AS

WITH rename_and_cast_comments AS (
    SELECT 
        -- 1. Ép kiểu khóa chính
        TRY_CAST(comment_id AS BIGINT) AS tiki_comment_id, 
        
        -- 2. Khóa ngoại nối với bảng sách
        TRY_CAST(product_id AS BIGINT) AS tiki_product_id, 
        TRY_CAST(customer_id AS BIGINT) AS tiki_customer_id, 
        
        -- 3. Các cột nội dung
        CAST(title AS NVARCHAR(500)) AS title,
        CAST(content AS NVARCHAR(MAX)) AS content,
        
        -- 4. Các con số đánh giá
        TRY_CAST(rating AS INT) AS rating,
        TRY_CAST(thank_count AS INT) AS thank_count
    FROM comments
),

deduplicate_comments AS (
    SELECT *,
           -- Đánh số thứ tự để lọc trùng
           ROW_NUMBER() OVER (
               PARTITION BY tiki_comment_id 
               ORDER BY (SELECT NULL) 
           ) AS rn
    FROM rename_and_cast_comments
    WHERE tiki_comment_id IS NOT NULL 
      AND tiki_comment_id <> 0 -- Bỏ ID rác bằng 0
      AND tiki_product_id IS NOT NULL -- [QUAN TRỌNG]: Bỏ comment không có chủ
)

-- Chỉ lấy dòng sạch nhất (rn=1)
SELECT 
    tiki_comment_id,
    tiki_product_id,
    tiki_customer_id,
    title,
    content,
    rating,
    thank_count
FROM deduplicate_comments
WHERE rn = 1

--SELECT * FROM VW_Comments_Clean

/* =============================================
   SUMMARY – COMMENTS DATA CLEANING
   - Removed duplicate comments by comment_id
   - Excluded comments without product reference
   - Standardized data types
   - Preserved original review content for sentiment analysis
   ============================================= */
