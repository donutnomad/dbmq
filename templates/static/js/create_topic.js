document.getElementById('createTopicForm').addEventListener('submit', async function(e) {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const topicData = {
        name: formData.get('topicName'),
        numPartitions: parseInt(formData.get('partitions')),
        config: {
            'retention.hours': parseInt(formData.get('retentionHours') || 168)
        }
    };

    if (formData.get('description')) {
        topicData.description = formData.get('description');
    }

    try {
        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(topicData)
        });

        const result = await response.json();
        
        if (result.success) {
            showMessage('Topic 创建成功！', 'success');
            setTimeout(() => {
                window.location.href = '/api/v1/dashboard';
            }, 2000);
        } else {
            showMessage('创建失败: ' + (result.error || '未知错误'), 'error');
        }
    } catch (error) {
        showMessage('网络错误: ' + error.message, 'error');
    }
});

function showMessage(text, type) {
    const messageDiv = document.getElementById('message');
    messageDiv.className = 'alert alert-' + type;
    messageDiv.textContent = text;
    messageDiv.style.display = 'block';
    
    if (type === 'success') {
        setTimeout(() => {
            messageDiv.style.display = 'none';
        }, 5000);
    }
} 