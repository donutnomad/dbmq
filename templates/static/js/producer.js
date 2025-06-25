let stats = { success: 0, error: 0, total: 0 };

// 页面加载时获取Topics列表
document.addEventListener('DOMContentLoaded', async function() {
    await loadTopics();
});

// 加载Topics列表
async function loadTopics() {
    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics');
        const result = await response.json();
        
        if (result.success && result.data) {
            const topicSelect = document.getElementById('topic');
            topicSelect.innerHTML = '<option value="">选择Topic...</option>';
            
            result.data.forEach(topic => {
                const option = document.createElement('option');
                option.value = topic.name || topic.topicName;
                option.textContent = topic.name || topic.topicName;
                topicSelect.appendChild(option);
            });
        } else {
            showMessage('获取Topics列表失败: ' + (result.error || '未知错误'), 'error');
        }
    } catch (error) {
        showMessage('网络错误: ' + error.message, 'error');
    }
}

// 表单提交处理
document.getElementById('producerForm').addEventListener('submit', async function(e) {
    e.preventDefault();
    await sendMessage();
});

// 发送消息
async function sendMessage() {
    const formData = new FormData(document.getElementById('producerForm'));
    const sendBtn = document.getElementById('sendBtn');
    
    // 验证表单
    const topic = formData.get('topic');
    const messageValue = formData.get('messageValue');
    
    if (!topic || !messageValue) {
        showMessage('请填写必需的字段（Topic和消息内容）', 'error');
        return;
    }

    // 解析headers
    let headers = {};
    const headersStr = formData.get('headers');
    if (headersStr) {
        try {
            headers = JSON.parse(headersStr);
        } catch (e) {
            showMessage('消息头格式错误，请使用有效的JSON格式', 'error');
            return;
        }
    }

    const batchCount = parseInt(formData.get('batchCount')) || 1;
    const interval = parseInt(formData.get('interval')) || 0;

    sendBtn.disabled = true;
    sendBtn.textContent = '发送中...';

    try {
        for (let i = 0; i < batchCount; i++) {
            const messageData = {
                topic: topic,
                key: formData.get('messageKey') || null,
                value: messageValue,
                headers: headers
            };

            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/' + encodeURIComponent(topic) + '/messages', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(messageData)
                });

                const result = await response.json();
                
                if (result.success) {
                    stats.success++;
                } else {
                    stats.error++;
                    console.error('Message send failed:', result.error);
                }
            } catch (error) {
                stats.error++;
                console.error('Network error:', error);
            }

            stats.total++;
            updateStats();

            // 等待间隔时间
            if (interval > 0 && i < batchCount - 1) {
                await new Promise(resolve => setTimeout(resolve, interval));
            }
        }

        showMessage('消息发送完成！成功: ' + stats.success + '，失败: ' + stats.error, 
                   stats.error === 0 ? 'success' : 'error');
                   
    } catch (error) {
        showMessage('发送失败: ' + error.message, 'error');
    } finally {
        sendBtn.disabled = false;
        sendBtn.textContent = '发送消息';
    }
}

// 更新统计显示
function updateStats() {
    document.getElementById('successCount').textContent = stats.success;
    document.getElementById('errorCount').textContent = stats.error;
    document.getElementById('totalCount').textContent = stats.total;
}

// 清空表单
function clearForm() {
    document.getElementById('producerForm').reset();
    stats = { success: 0, error: 0, total: 0 };
    updateStats();
}

// 加载示例消息
function loadSampleMessage() {
    document.getElementById('messageKey').value = 'sample-key-' + Date.now();
    document.getElementById('messageValue').value = JSON.stringify({
        id: Date.now(),
        message: "这是一个示例消息",
        timestamp: new Date().toISOString(),
        data: {
            user: "用户123",
            action: "示例操作"
        }
    }, null, 2);
    document.getElementById('headers').value = JSON.stringify({
        "source": "dashboard",
        "version": "1.0"
    }, null, 2);
}

// 显示消息
function showMessage(text, type) {
    const messageDiv = document.getElementById('message');
    messageDiv.className = 'alert alert-' + type;
    messageDiv.textContent = text;
    messageDiv.style.display = 'block';
    
    setTimeout(() => {
        messageDiv.style.display = 'none';
    }, 5000);
} 