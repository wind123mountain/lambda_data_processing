#!/bin/bash

# --- BƯỚC 1: Format Namenode (Chỉ chạy lần đầu) ---
# Kiểm tra nếu thư mục Namenode đã được format
if [ ! -d /tmp/hadoop-root/dfs/name/current ]; then
  echo "Formatting Namenode..."
  hdfs namenode -format -force
fi

# --- BƯỚC 2: Chạy các dịch vụ Hadoop/YARN ở chế độ nền ---

# 1. Khởi động Namenode
echo "Starting Namenode..."
hdfs namenode &

# 2. Khởi động Datanode (Nên chạy sau Namenode)
echo "Starting Datanode..."
hdfs datanode &

# 3. Khởi động ResourceManager
echo "Starting ResourceManager..."
yarn resourcemanager &

# 4. Chạy các script rewrite cần thiết (nếu có)
# Ví dụ: /rewrite_mapred_site.sh

# 5. Khởi động Nodemanager
echo "Starting Nodemanager..."
yarn nodemanager &

# --- BƯỚC 3: Giữ Container chạy ---
# Lệnh này giữ cho script và container chạy liên tục
# Nó sẽ tail log của Namenode/Datanode hoặc chỉ giữ terminal mở
echo "All services started. Keeping container alive..."
wait # Đợi cho tất cả các tiến trình con (Namenode, Datanode, v.v.) kết thúc.

# Một cách khác để giữ container chạy là:
# tail -f /dev/null