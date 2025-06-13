-- =====================================
-- Analisis Penjualan The Look eCommerce
-- Dataset: bigquery-public-data.thelook_ecommerce
-- Dibuat oleh: Fia0530
-- =====================================

-- Mengambil 10 Produk terlaris di TheLook eCommerce
SELECT products.name, COUNT(*) as total_sold 
FROM `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
JOIN `bigquery-public-data.thelook_ecommerce.products` as products 
ON order_items.order_id=products.id 
WHERE order_items.status != 'Returned' OR order_items.status != 'Cancelled' 
GROUP BY products.name 
ORDER BY total_sold DESC 
LIMIT 10 

-- Mengambil 5 brand dengan produk terbanyak di TheLook eCommerce
SELECT brand, COUNT(*) as total_product 
FROM `bigquery-public-data.thelook_ecommerce.products` 
GROUP BY brand 
ORDER BY total_product DESC 
LIMIT 5

-- Mengambil Sumber trafik paling efektif yang digunakan pelanggan 
SELECT traffic_source, COUNT(*) as jumlah_kunjungan 
FROM `bigquery-public-data.thelook_ecommerce.users` 
GROUP BY traffic_source 
ORDER BY jumlah_kunjungan DESC 

-- Mengambil Produk yang sering dibatal dan dikembalikan
SELECT products.name,  COUNT(CASE WHEN order_items.status = 'Returned' THEN 1 
END) AS Return, COUNT(CASE WHEN order_items.status = 'Cancelled' THEN 1 END) 
AS Cancel, COUNT(*) as total 
FROM `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
JOIN `bigquery-public-data.thelook_ecommerce.products` as products 
ON order_items.order_id=products.id 
WHERE order_items.status = 'Returned' OR order_items.status = 'Cancelled' 
GROUP BY products.name 
ORDER BY total DESC, Return DESC, Cancel DESC 
LIMIT 10 

-- Mengambil Top 10 kategori produk terbanyak yang dibeli menurut wilayah 
SELECT users.state,products.category, COUNT(*) as jumlah_beli 
FROM `bigquery-public-data.thelook_ecommerce.products` as products 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON products.id = order_items.product_id 
JOIN `bigquery-public-data.thelook_ecommerce.users` as users 
ON order_items.user_id = users.id 
GROUP BY users.state, products.category 
ORDER BY jumlah_beli DESC 
LIMIT 10 

-- Mengambil Preferensi kategori produk berdasarkan wilayah
WITH ranked_products AS ( 
SELECT users.state AS wilayah,products.category AS kategori_produk,COUNT(*) 
AS jumlah_beli, 
ROW_NUMBER() OVER(PARTITION BY users.state ORDER BY COUNT(*) DESC) AS rank 
FROM `bigquery-public-data.thelook_ecommerce.products` AS products 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS order_items 
ON products.id = order_items.product_id 
JOIN `bigquery-public-data.thelook_ecommerce.users` AS users 
ON order_items.user_id = users.id 
GROUP BY users.state, products.category 
) 
SELECT wilayah,kategori_produk,jumlah_beli 
FROM ranked_products 
WHERE rank = 1; 

-- Mengambil Preferensi kategori produk paling sering dibeli berdasarkan jenjang usia pelanggan
WITH ranked_products as ( 
SELECT users.age as usia,products.category as kategori_produk,COUNT(*) as 
jumlah_beli, 
ROW_NUMBER() OVER(PARTITION BY users.age ORDER BY COUNT(*) DESC) as rank 
FROM `bigquery-public-data.thelook_ecommerce.products` as products 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON products.id = order_items.product_id 
JOIN `bigquery-public-data.thelook_ecommerce.users` as users 
ON order_items.user_id = users.id 
GROUP BY users.age, products.category 
ORDER BY users.age 
) 
SELECT usia,kategori_produk,jumlah_beli 
FROM ranked_products 
WHERE rank = 1; 

-- Mengambil Sumber lalu lintas paling efektif yang mengarahkan pelanggan menuju ke laman The Look eCommerce
SELECT traffic_source, COUNT(*) as jumlah_pengunjung 
FROM `bigquery-public-data.thelook_ecommerce.events` 
GROUP BY traffic_source 
ORDER BY jumlah_pengunjung DESC

-- Mengambil Waktu pembelian tersibuk 
SELECT EXTRACT(HOUR FROM created_at) AS jam_pembelian, COUNT(*) AS 
total_pembelian 
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
GROUP BY jam_pembelian 
ORDER BY total_pembelian DESC; 

-- Mengambil Usia terbanyak pelanggan TheLook eCommerce
SELECT age, COUNT(*) as jumlah_pelanggan 
FROM `bigquery-public-data.thelook_ecommerce.users`  
GROUP BY age 
ORDER BY jumlah_pelanggan DESC

-- Mengambil Perilaku gender perempuan dalam pembelian produk 
SELECT products.category, COUNT(*) as total_pembelian 
FROM `bigquery-public-data.thelook_ecommerce.orders` as orders 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON order_items.id=orders.order_id 
JOIN `bigquery-public-data.thelook_ecommerce.products` as products 
ON order_items.product_id=products.id 
WHERE orders.gender='F' 
GROUP BY products.category 
ORDER BY total_pembelian DESC 

-- Mengambil Perilaku gender laki-laki dalam pembelian produk 
SELECT products.category, COUNT(*) as total_pembelian 
FROM `bigquery-public-data.thelook_ecommerce.orders` as orders 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON order_items.id=orders.order_id 
JOIN `bigquery-public-data.thelook_ecommerce.products` as products 
ON order_items.product_id=products.id 
WHERE orders.gender='M' 
GROUP BY products.category 
ORDER BY total_pembelian DESC 

-- Mengambil Rata-rata waktu pengiriman produk 
SELECT AVG(TIMESTAMP_DIFF(delivered_at, shipped_at,DAY)) as 
rata_rata_waktu_pengiriman_hari 
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
WHERE shipped_at IS NOT NULL; 

-- Mengambil Brand paling disenangi oleh pelanggan 
SELECT products.brand, COUNT(*) as total_pembelian 
FROM `bigquery-public-data.thelook_ecommerce.products` as products 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON products.id= order_items.product_id 
GROUP BY products.brand 
ORDER BY total_pembelian DESC

-- Mengambil Lokasi pelanggan tertinggi pemesan produk
SELECT users.state, COUNT(*) as total_pembelian 
FROM `bigquery-public-data.thelook_ecommerce.users` as users 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as order_items 
ON users.id= order_items.user_id 
GROUP BY users.state 
ORDER BY total_pembelian DESC



