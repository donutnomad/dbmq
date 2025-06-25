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

// 获取状态徽章
function getStatusBadge(status) {
    const statusMap = {
        'Active': { class: 'metric-success', text: '活跃' },
        'Stable': { class: 'metric-success', text: '稳定' },
        'Empty': { class: 'metric-info', text: '空闲' },
        'Dead': { class: 'metric-warning', text: '停止' }
    };
    
    const statusInfo = statusMap[status] || { class: 'metric-info', text: status || '未知' };
    return '<span class="metric-badge ' + statusInfo.class + '">' + statusInfo.text + '</span>';
}

// 格式化时间
function formatTime(timeStr) {
    if (!timeStr) return '--';
    try {
        const date = new Date(timeStr);
        return date.toLocaleString('zh-CN');
    } catch (e) {
        return timeStr;
    }
}

// 加载消费组详情
async function loadConsumerGroupDetail() {
    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/consumer-groups/' + groupId);
        const result = await response.json();
        
        if (result.success) {
            updateConsumerGroupDetail(result.data);
        } else {
            showError(result.error || '获取消费组详情失败');
        }
    } catch (error) {
        showError('网络错误: ' + error.message);
    }
}

// 更新消费组详情显示
function updateConsumerGroupDetail(data) {
    // 更新基本信息
    document.getElementById('state').innerHTML = getStatusBadge(data.state);
    document.getElementById('memberCount').textContent = data.members ? data.members.length : 0;
    document.getElementById('lag').textContent = formatNumber(data.lag || 0);
    document.getElementById('generationId').textContent = data.generationId || '--';

    // 更新成员表格
    const membersTable = document.getElementById('membersTable');
    membersTable.innerHTML = '';
    
    if (data.members && data.members.length > 0) {
        data.members.forEach(member => {
            const row = document.createElement('tr');
            row.innerHTML = 
                '<td>' + (member.consumerId || '--') + '</td>' +
                '<td>' + (member.clientId || '--') + '</td>' +
                '<td>' + (member.host || '--') + '</td>' +
                '<td>' + (member.assignment ? member.assignment.length : 0) + '</td>' +
                '<td>' + formatTime(member.lastHeartbeat) + '</td>';
            membersTable.appendChild(row);
        });
    } else {
        membersTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">暂无成员数据</td></tr>';
    }

    // 更新分区延迟表格
    const partitionLagsTable = document.getElementById('partitionLagsTable');
    partitionLagsTable.innerHTML = '';
    
    if (data.partitionLags && data.partitionLags.length > 0) {
        data.partitionLags.forEach(lag => {
            const row = document.createElement('tr');
            row.innerHTML = 
                '<td>' + (lag.topic || '--') + '</td>' +
                '<td>' + (lag.partition !== undefined ? lag.partition : '--') + '</td>' +
                '<td>' + formatNumber(lag.currentOffset) + '</td>' +
                '<td>' + formatNumber(lag.latestOffset) + '</td>' +
                '<td>' + formatNumber(lag.lag) + '</td>';
            partitionLagsTable.appendChild(row);
        });
    } else {
        partitionLagsTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">暂无延迟数据</td></tr>';
    }

    // 更新分配的Topics
    const assignedTopicsDiv = document.getElementById('assignedTopics');
    if (data.assignedTopics && data.assignedTopics.length > 0) {
        assignedTopicsDiv.innerHTML = data.assignedTopics.map(topic => 
            '<span class="topic-badge">' + topic + '</span>'
        ).join('');
    } else {
        assignedTopicsDiv.innerHTML = '<span style="color: #666;">暂无分配的Topics</span>';
    }

    // 显示内容，隐藏加载动画
    document.getElementById('loading').style.display = 'none';
    document.getElementById('content').style.display = 'block';
}

// 显示错误
function showError(message) {
    document.getElementById('errorMessage').textContent = message;
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'block';
}

// 页面加载完成后加载数据
document.addEventListener('DOMContentLoaded', loadConsumerGroupDetail); 