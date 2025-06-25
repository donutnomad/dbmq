// 格式化数字显示
function formatNumber(num) {
    if (num === undefined || num === null) return '--';
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// 格式化字节大小
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// 加载Topic详情
async function loadTopicDetail() {
    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/' + topicName);
        const result = await response.json();
        
        if (result.success) {
            updateTopicDetail(result.data);
        } else {
            showError(result.error || '获取Topic详情失败');
        }
    } catch (error) {
        showError('网络错误: ' + error.message);
    }
}

// 更新Topic详情显示
function updateTopicDetail(data) {
    // 更新基本信息
    document.getElementById('partitionCount').textContent = data.partitionCount || 0;
    document.getElementById('messageCount').textContent = formatNumber(data.messageCount || 0);
    document.getElementById('sizeBytes').textContent = formatBytes(data.sizeBytes || 0);
    document.getElementById('latestOffset').textContent = formatNumber(data.latestOffset || 0);

    // 更新分区详情表格
    const partitionsTable = document.getElementById('partitionsTable');
    partitionsTable.innerHTML = '';
    
    if (data.partitions && data.partitions.length > 0) {
        data.partitions.forEach(partition => {
            const row = document.createElement('tr');
            row.innerHTML = 
                '<td>' + partition.partition + '</td>' +
                '<td>' + formatNumber(partition.latestOffset || 0) + '</td>' +
                '<td>' + formatNumber(partition.messageCount || 0) + '</td>' +
                '<td>' + formatBytes(partition.sizeBytes || 0) + '</td>';
            partitionsTable.appendChild(row);
        });
    } else {
        partitionsTable.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无分区数据</td></tr>';
    }

    // 更新配置表格
    const configTable = document.getElementById('configTable');
    configTable.innerHTML = '';
    
    if (data.config && Object.keys(data.config).length > 0) {
        Object.entries(data.config).forEach(([key, value]) => {
            const row = document.createElement('tr');
            row.innerHTML = 
                '<td>' + key + '</td>' +
                '<td>' + value + '</td>';
            configTable.appendChild(row);
        });
    } else {
        configTable.innerHTML = '<tr><td colspan="2" style="text-align: center; color: #666;">暂无配置数据</td></tr>';
    }

    // 初始化分区选择器
    initializePartitionSelect(data.partitions);

    // 显示内容，隐藏加载动画
    document.getElementById('loading').style.display = 'none';
    document.getElementById('content').style.display = 'block';

    // 自动加载最新消息
    loadMessages();
}

// 显示错误
function showError(message) {
    document.getElementById('errorMessage').textContent = message;
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'block';
}

// 初始化分区选择器
function initializePartitionSelect(partitions) {
    const partitionSelect = document.getElementById('partitionSelect');
    partitionSelect.innerHTML = '<option value="">所有分区</option>';
    
    if (partitions && partitions.length > 0) {
        partitions.forEach(partition => {
            const option = document.createElement('option');
            option.value = partition.partition;
            option.textContent = '分区 ' + partition.partition;
            partitionSelect.appendChild(option);
        });
    }
}

// 加载消息
async function loadMessages() {
    const messagesLoading = document.getElementById('messagesLoading');
    const messagesTable = document.getElementById('messagesTable');
    
    messagesLoading.style.display = 'block';
    messagesTable.innerHTML = '';

    try {
        const partition = document.getElementById('partitionSelect').value;
        const search = document.getElementById('searchInput').value;
        const limit = document.getElementById('limitSelect').value;

        let url = '/api/v1/dbmq/topics/' + topicName + '/messages?limit=' + limit;
        if (partition) url += '&partition=' + partition;
        if (search) url += '&search=' + encodeURIComponent(search);

        const response = await fetch(url);
        const result = await response.json();
        
        if (result.success) {
            displayMessages(result.data.messages);
        } else {
            showError(result.error || '获取消息失败');
        }
    } catch (error) {
        showError('网络错误: ' + error.message);
    } finally {
        messagesLoading.style.display = 'none';
    }
}

// 显示消息列表
function displayMessages(messages) {
    const messagesTable = document.getElementById('messagesTable');
    
    if (!messages || messages.length === 0) {
        messagesTable.innerHTML = '<div class="no-messages">📭 暂无消息数据</div>';
        return;
    }

    let html = '';
    messages.forEach((msg, index) => {
        const timestamp = new Date(msg.timestamp).toLocaleString('zh-CN');
        const msgKey = msg.key || '(无键)';
        const msgValue = msg.value || '';
        html += '<div class="message-item">' +
            '<div class="message-header" onclick="toggleMessage(' + index + ')">' +
                '<div>' +
                    '<span class="message-key">' + msgKey + '</span>' +
                    '<div class="message-meta">' +
                        '<span>分区: ' + msg.partition + '</span>' +
                        '<span>偏移量: ' + msg.offset + '</span>' +
                        '<span>时间: ' + timestamp + '</span>' +
                        '<span>大小: ' + formatBytes(msg.size) + '</span>' +
                    '</div>' +
                '</div>' +
                '<span id="toggle-' + index + '">🔼</span>' +
            '</div>' +
            '<div class="message-content expanded" id="content-' + index + '">' +
                '<div class="message-value">' + msgValue + '</div>' +
            '</div>' +
        '</div>';
    });
    
    messagesTable.innerHTML = html;
}

// 切换消息展开/折叠
function toggleMessage(index) {
    const content = document.getElementById('content-' + index);
    const toggle = document.getElementById('toggle-' + index);
    
    if (content.classList.contains('expanded')) {
        content.classList.remove('expanded');
        toggle.textContent = '🔽';
    } else {
        content.classList.add('expanded');
        toggle.textContent = '🔼';
    }
}

// 绑定事件监听器
document.addEventListener('DOMContentLoaded', function() {
    loadTopicDetail();

    // 搜索按钮事件
    document.getElementById('searchBtn').addEventListener('click', loadMessages);
    
    // 刷新按钮事件
    document.getElementById('refreshBtn').addEventListener('click', loadMessages);
    
    // 回车搜索
    document.getElementById('searchInput').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            loadMessages();
        }
    });
}); 