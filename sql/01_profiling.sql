/*
Project: Tiki Books Sales & Reviews Analysis
File: 01_data_profiling.sql
Purpose: Exploratory data profiling for book_data and comments tables
Dataset: Tiki Books from Kaggle (book_data: 1367 rows, comments: 136k rows)
Key findings: Duplicates in keys, missing values (pages, rating, product_id), outliers (price=0, quantity>600k), scraped errors (category wrong, authors ".").
*/

/* =============================================
   PROFILING BOOK_DATA TABLE
   Primary key: product_id
   Total records: 1367 
   ============================================= */

-- Tổng quan rows
SELECT COUNT(*) AS total_records FROM book_data;
-- Insight: 1367 rows 

-- Tổng quan missing values 
SELECT 
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(product_id) AS missing_product_id,
  COUNT(*) - COUNT(title) AS missing_title,
  COUNT(*) - COUNT(authors) AS missing_authors,
  COUNT(*) - COUNT(original_price) AS missing_original_price,
  COUNT(*) - COUNT(current_price) AS missing_current_price,
  COUNT(*) - COUNT(quantity) AS missing_quantity,
  COUNT(*) - COUNT(category) AS missing_category,
  COUNT(*) - COUNT(n_review) AS missing_n_review,
  COUNT(*) - COUNT(avg_rating) AS missing_avg_rating,
  COUNT(*) - COUNT(pages) AS missing_pages,
  COUNT(*) - COUNT(manufacturer) AS missing_manufacturer,
  COUNT(*) - COUNT(cover_link) AS missing_cover_link
FROM book_data;
-- Insight: Phát hiện missing ở pages, quantity

-- COLUMN product_id (Primary key)
SELECT product_id, COUNT(*) AS count_occurrence
FROM book_data
GROUP BY product_id
HAVING COUNT(*) > 1;
-- Insight: 16 duplicate rows → cần drop duplicate dựa trên product_id ở cleaning

-- COLUMN title => Ổn

-- COLUMN authors
SELECT authors, COUNT(*) AS count
FROM book_data
GROUP BY authors
ORDER BY count DESC
LIMIT 20;
-- Insight: Có "." và ".." → giá trị lỗi → replace bằng 'Unknown' hoặc NULL

-- COLUMN original_price & current_price
SELECT 
  MIN(original_price) AS min_original, MAX(original_price) AS max_original,
  MIN(current_price) AS min_current, MAX(current_price) AS max_current
FROM book_data;
-- Insight: Giá min = 0đ → outlier/lỗi scrape → filter hoặc replace ở cleaning

-- COLUMN quantity
SELECT 
  MIN(quantity) AS min_qty, MAX(quantity) AS max_qty,
  AVG(quantity) AS avg_qty
FROM book_data;
SELECT quantity, COUNT(*) FROM book_data GROUP BY quantity ORDER BY quantity LIMIT 20;
-- Insight: Min ~1 và max >600k suspicious, có null → cap outlier (99th percentile) hoặc fill median

-- COLUMN category
SELECT category, COUNT(*) AS count_books
FROM book_data
GROUP BY category
ORDER BY count_books DESC
LIMIT 20;
-- Insight: Một số category hiển thị tên sách → lỗi parse → cần manual clean hoặc drop

-- COLUMN n_review
SELECT n_review, COUNT(*) 
FROM book_data 
GROUP BY n_review 
ORDER BY n_review DESC 
LIMIT 10;
-- Insight: Top 1 có review gấp đôi → potential bestseller hoặc lỗi → giữ nhưng note

-- COLUMN avg_rating
SELECT avg_rating, COUNT(*) 
FROM book_data 
GROUP BY avg_rating 
ORDER BY avg_rating DESC;
-- Insight: Có 0* → chưa có review → fill bằng overall mean hoặc flag

-- COLUMN pages
SELECT pages, COUNT(*) 
FROM book_data 
GROUP BY pages 
ORDER BY pages;
-- Insight: Nhiều null → fill bằng median theo category nếu cần

-- COLUMN manufacturer
SELECT manufacturer, COUNT(*) 
FROM book_data 
GROUP BY manufacturer 
ORDER BY COUNT(*) DESC;
-- Insight: Duplicate tên nhưng khác viết hoa/thường, có trống → standardize và fill 'Unknown'

-- COLUMN cover_link => Ổn


/* =============================================
   PROFILING COMMENTS TABLE
   Primary key: comment_id 
   Total records: 136162
   ============================================= */

-- Tổng quan rows 
SELECT COUNT(*) AS total_records FROM comments;
-- Insight: 136162 rows 

-- Tổng quan missing values 
SELECT 
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(comment_id) AS missing_comment_id,
  COUNT(*) - COUNT(product_id) AS missing_product_id,
  COUNT(*) - COUNT(title) AS missing_title,
  COUNT(*) - COUNT(thank_count) AS missing_thank_count,
  COUNT(*) - COUNT(customer_id) AS missing_customer_id,
  COUNT(*) - COUNT(rating) AS missing_rating,
  COUNT(*) - COUNT(content) AS missing_content
FROM comments;
-- Insight: Null ở product_id → sẽ mất khi JOIN book_data; 
-- null rating → fill bằng mean hoặc drop; check content null để drop review rác


-- COLUMN comment_id (Primary key)
SELECT comment_id, COUNT(*) AS count_occurrence
FROM comments
GROUP BY comment_id
HAVING COUNT(*) > 1;
-- Insight: Có duplicate → nghiêm trọng với primary key → drop duplicate ở cleaning (keep first)

-- COLUMN product_id (Foreign key)
SELECT product_id, COUNT(*) AS num_comments_per_product
FROM comments
GROUP BY product_id
ORDER BY num_comments_per_product DESC
LIMIT 20;
-- Insight: Có null → review không link được sách → drop rows null product_id khi JOIN
-- Bonus: Top product có hàng nghìn comments → bestseller tiềm năng

-- COLUMN title => Ổn

-- COLUMN thank_count
SELECT 
  MIN(thank_count) AS min_thank,
  MAX(thank_count) AS max_thank,
  AVG(thank_count) AS avg_thank
FROM comments;
SELECT thank_count, COUNT(*) AS count
FROM comments
GROUP BY thank_count
ORDER BY count DESC
LIMIT 20;
-- Insight: Phần lớn thank_count = 0 → review ít hữu ích; có outlier cao → review chất lượng tốt

-- COLUMN customer_id => Ổn

-- COLUMN rating
SELECT 
  MIN(rating) AS min_rating, MAX(rating) AS max_rating,
  AVG(rating) AS avg_rating
FROM comments;
SELECT rating, COUNT(*) AS count
FROM comments
GROUP BY rating
ORDER BY count DESC;
-- Insight: Có null → fill bằng overall mean hoặc drop; distribution thường tập trung 4-5 sao (typical review VN)

-- COLUMN content => Ổn
