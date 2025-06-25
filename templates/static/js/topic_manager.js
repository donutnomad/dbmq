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
    if (bytes === 0 || !bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// 加载Topics
async function loadTopics() {
    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics');
        const result = await response.json();
        
        if (result.success) {
            displayTopics(result.data);
        } else {
            alert('获取Topics失败: ' + (result.error || '未知错误'));
        }
    } catch (error) {
        alert('网络错误: ' + error.message);
    } finally {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('content').style.display = 'block';
    }
}

// 显示Topics
function displayTopics(topics) {
    const tbody = document.getElementById('topicsTable');
    tbody.innerHTML = '';
    
    if (!topics || topics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #666;">暂无 Topic 数据</td></tr>';
        return;
    }

    topics.forEach(topic => {
        const row = document.createElement('tr');
        const topicName = topic.name || topic.topicName || '--';
        row.innerHTML = 
            '<td><a href="/api/v1/dashboard/topic/' + encodeURIComponent(topicName) + '" class="topic-link">' + topicName + '</a></td>' +
            '<td>' + (topic.partitionCount || '--') + '</td>' +
            '<td>' + formatNumber(topic.messageCount || 0) + '</td>' +
            '<td>' + formatBytes(topic.sizeBytes || 0) + '</td>' +
            '<td>' + (topic.createdAt ? new Date(topic.createdAt).toLocaleString('zh-CN') : '--') + '</td>' +
            '<td><button class="btn-danger" onclick="deleteTopic(\'' + topicName + '\')">删除</button></td>';
        tbody.appendChild(row);
    });
}

// 删除Topic
async function deleteTopic(topicName) {
    if (!confirm('确定要删除 Topic "' + topicName + '" 吗？此操作不可撤销！')) {
        return;
    }

    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/' + encodeURIComponent(topicName), {
            method: 'DELETE'
        });

        const result = await response.json();
        
        if (result.success) {
            alert('Topic 删除成功！');
            loadTopics(); // 重新加载列表
        } else {
            alert('删除失败: ' + (result.error || '未知错误'));
        }
    } catch (error) {
        alert('网络错误: ' + error.message);
    }
}

// 页面加载完成后加载数据
document.addEventListener('DOMContentLoaded', loadTopics); 