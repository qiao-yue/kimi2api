# kimi2api
本项目仅供学习！
利用kimi2.6agent编写的kimi api，兼容OpenAI协议，聊天模型【支持K2.6和k3模型】，不支持工具调用。但是你可以让他输出工具调用的格式，然后自行处理

---

## 交付文件


| 文件 | 说明 |
|------|------|
| `web_server.py` | **网页版逆向代理**（Connect Protocol 编解码 + 协议转换） |
| `requirements.txt` | 依赖 |
| `README.md` | 完整文档 |

### 使用方法

```bash
# 1. 安装依赖
pip install -r requirements.txt
# 2. 获取 access_token（从 kimi.com localStorage）
# 可在浏览器打开kimi聊天页面，按F12,选择应用 - 存储里的Cookie - https://www.kimi.com - kimi-auth,
# 把值复制到系统变量中（设置 - 系统 - 系统信息 - 高级系统设置 - 环境变量 - 用户环境变量中点击新建 ），变量名为：KIMI_ACCESS_TOKEN
export KIMI_ACCESS_TOKEN="eyJhbGciOiJIUzUxMi..."

# 3. 启动网页版代理
python web_server.py --port 7100

```

### 已知限制

- `access_token` 有过期时间（JWT payload 中的 `exp` 字段），过期后需要重新获取（有效期1年）
- 网页版协议可能随时更新，需要持续维护
- 仅供学习
- 请求头中的 `x-msh-device-id` 是硬编码的，可能需要从 token 中动态提取

**需要我帮你测试 web_server.py 的实际连通性，或者进一步完善历史对话支持吗？**
