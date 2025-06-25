let refreshInterval;
let isAutoRefreshEnabled = true;
let chartData = {
    timestamps: []
};

// 切换自动刷新状态
function toggleAutoRefresh(enabled) {
    isAutoRefreshEnabled = enabled;
    if (enabled) {
        loadDashboardData();
        refreshInterval = setInterval(loadDashboardData, 5000);
        document.getElementById('refreshStatus').textContent = '页面每 5 秒自动刷新一次';
    } else {
        if (refreshInterval) {
            clearInterval(refreshInterval);
        }
        document.getElementById('refreshStatus').textContent = '自动刷新已关闭';
    }
}

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

// 格式化运行时间
function formatUptime(seconds) {
    if (!seconds) return '--';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    
    if (days > 0) {
        return days + '天 ' + hours + '小时';
    } else if (hours > 0) {
        return hours + '小时 ' + minutes + '分钟';
    } else {
        return minutes + '分钟';
    }
}

// 获取状态徽章
function getStatusBadge(status) {
    const statusMap = {
        'active': { class: 'metric-success', text: '活跃' },
        'stable': { class: 'metric-success', text: '稳定' },
        'empty': { class: 'metric-info', text: '空闲' },
        'dead': { class: 'metric-warning', text: '停止' }
    };
    
    const statusInfo = statusMap[status] || { class: 'metric-info', text: status || '未知' };
    return '<span class="metric-badge ' + statusInfo.class + '">' + statusInfo.text + '</span>';
}

// 加载仪表板数据
async function loadDashboardData() {
    try {
        const response = await fetch('/api/v1/dashboard/data');
        const result = await response.json();
        
        if (result.success) {
            updateDashboard(result.data);
        } else {
            console.error('Failed to load dashboard data:', result.error);
        }
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

// 更新仪表板显示
function updateDashboard(data) {
    // 更新统计数据
    document.getElementById('topicCount').textContent = data.topics ? data.topics.length : 0;
    document.getElementById('consumerGroupCount').textContent = data.consumerGroups ? data.consumerGroups.length : 0;
    
    let totalMessages = 0;
    if (data.topics) {
        data.topics.forEach(topic => {
            if (topic.messageCount) {
                totalMessages += topic.messageCount;
            }
        });
    }
    document.getElementById('totalMessages').textContent = formatNumber(totalMessages);
    
    if (data.system && data.system.uptime) {
        document.getElementById('uptime').textContent = formatUptime(data.system.uptime);
    }

    // 更新 Topics 表格
    updateTopicsTable(data.topics || []);
    
    // 更新消费组表格
    updateConsumerGroupsTable(data.consumerGroups || []);
    
    // 更新最后更新时间
    document.getElementById('lastUpdate').textContent = new Date().toLocaleString('zh-CN');
}

// 更新 Topics 表格
function updateTopicsTable(topics) {
    const tbody = document.getElementById('topicsTable');
    tbody.innerHTML = '';
    
    if (topics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无 Topic 数据</td></tr>';
    } else {
        topics.forEach(topic => {
            const topicName = topic.name || topic.topicName || '--';
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.onclick = function(e) {
                e.preventDefault();
                window.location.href = '/api/v1/dashboard/topic/' + encodeURIComponent(topicName);
            };
            row.innerHTML = 
                '<td><a href="/api/v1/dashboard/topic/' + encodeURIComponent(topicName) + '" class="topic-link" onclick="event.stopPropagation()">' + topicName + '</a></td>' +
                '<td>' + (topic.partitionCount || topic.partitions?.length || '--') + '</td>' +
                '<td>' + formatNumber(topic.messageCount || 0) + '</td>' +
                '<td>' + getStatusBadge(topic.status || 'active') + '</td>';
            tbody.appendChild(row);
        });
    }
    
    // 显示内容，隐藏加载动画
    document.getElementById('topicsLoading').style.display = 'none';
    document.getElementById('topicsContent').style.display = 'block';
}

// 更新消费组表格
function updateConsumerGroupsTable(consumerGroups) {
    const tbody = document.getElementById('consumersTable');
    tbody.innerHTML = '';
    
    if (consumerGroups.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无消费组数据</td></tr>';
    } else {
        consumerGroups.forEach(group => {
            const groupId = group.groupId || group.name || '--';
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.onclick = function(e) {
                e.preventDefault();
                window.location.href = '/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId);
            };
            row.innerHTML = 
                '<td><a href="/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId) + '" class="topic-link" onclick="event.stopPropagation()">' + groupId + '</a></td>' +
                '<td>' + getStatusBadge(group.state || group.status || 'unknown') + '</td>' +
                '<td>' + (group.memberCount || group.members?.length || 0) + '</td>' +
                '<td>' + formatNumber(group.lag || 0) + '</td>';
            tbody.appendChild(row);
        });
    }
    
    // 显示内容，隐藏加载动画
    document.getElementById('consumersLoading').style.display = 'none';
    document.getElementById('consumersContent').style.display = 'block';
}

// 初始化页面
function init() {
    loadDashboardData();
    
    // 设置定时刷新（5秒）
    refreshInterval = setInterval(loadDashboardData, 5000);

    // 绑定自动刷新开关事件
    document.getElementById('autoRefreshToggle').addEventListener('change', function(e) {
        toggleAutoRefresh(e.target.checked);
    });
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', init);

// 页面隐藏时清除定时器，显示时重新设置
document.addEventListener('visibilitychange', function() {
    const autoRefreshToggle = document.getElementById('autoRefreshToggle');
    if (document.hidden) {
        if (refreshInterval) {
            clearInterval(refreshInterval);
        }
    } else {
        if (autoRefreshToggle.checked) {
            loadDashboardData();
            refreshInterval = setInterval(loadDashboardData, 5000);
        }
    }
}); 