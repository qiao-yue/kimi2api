"""
Kimi2OpenAI-Web v2.2
- 思考内容用 ***思考开始***/***思考结束*** 包裹
- 自动删除临时会话，防止会话爆炸
"""

import os
import sys
import json
import time
import uuid
import struct
import argparse
from typing import AsyncGenerator, Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

try:
    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import StreamingResponse, JSONResponse
    import uvicorn
    import httpx
except ImportError:
    print("请先安装依赖: pip install fastapi uvicorn httpx")
    sys.exit(1)


# =============================================================================
# Connect Protocol 编解码器
# =============================================================================

class ConnectCodec:
    @staticmethod
    def encode_frame(data: dict, flag: int = 0x00) -> bytes:
        payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
        return struct.pack('>B', flag) + struct.pack('>I', len(payload)) + payload
    
    @staticmethod
    def decode_frames(data: bytes) -> Tuple[List[Dict[str, Any]], int]:
        frames = []
        i = 0
        while i < len(data):
            if i + 5 > len(data):
                break
            flag = data[i]
            length = struct.unpack('>I', data[i+1:i+5])[0]
            if length == 0:
                i += 5
                continue
            if i + 5 + length > len(data):
                break
            payload = data[i+5:i+5+length]
            try:
                frames.append({
                    'flag': flag,
                    'flag_name': {0: 'DATA', 1: 'TRAILER', 2: 'ERROR'}.get(flag, f'UNKNOWN({flag})'),
                    'data': json.loads(payload.decode('utf-8'))
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                frames.append({
                    'flag': flag,
                    'flag_name': 'PARSE_ERROR',
                    'raw_hex': payload[:50].hex()
                })
            i += 5 + length
        return frames, i


# =============================================================================
# 配置
# =============================================================================

@dataclass
class Config:
    host: str = "0.0.0.0"
    port: int = 7100
    kimi_base_url: str = "https://www.kimi.com"
    access_token: Optional[str] = None
    scenario: str = "SCENARIO_K2D5"
    thinking: bool = True
    enable_plugin: bool = True
    reasoning_effort: str = "REASONING_EFFORT_LOW"
    tools: Optional[List[Dict]] = None
    auto_delete_chat: bool = True  # 自动删除临时会话
    
    def __post_init__(self):
        if self.tools is None:
            self.tools = [
                {"type": "TOOL_TYPE_SEARCH", "search": {}},
                {"type": "TOOL_TYPE_CRON_JOB"}
            ]


# =============================================================================
# OpenAI <-> Kimi 协议转换 (v2.2)
# =============================================================================

class ProtocolConverter:
    
    @staticmethod
    def openai_to_kimi_request(messages: List[Dict], config: Config) -> bytes:
        last_user_msg = None
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str):
                    last_user_msg = content
                elif isinstance(content, list):
                    texts = [item.get("text", "") for item in content 
                             if isinstance(item, dict) and item.get("type") == "text"]
                    last_user_msg = " ".join(texts)
                break
        if last_user_msg is None:
            last_user_msg = "Hello"
        
        return ConnectCodec.encode_frame({
            "scenario": config.scenario,
            "tools": config.tools,
            "message": {
                "role": "user",
                "blocks": [{"message_id": "", "text": {"content": last_user_msg}}],
                "scenario": config.scenario,
                "is_goal": False
            },
            "options": {
                "thinking": config.thinking,
                "enable_plugin": config.enable_plugin,
                "reasoning_effort": config.reasoning_effort
            },
            "project_id": ""
        })
    
    @staticmethod
    def kimi_to_openai_stream(
        kimi_frames: List[Dict],
        model: str,
        completion_id: str,
        think_state: Optional[Dict] = None
    ) -> List[str]:
        """
        将 Kimi 响应帧转换为 OpenAI SSE 格式
        think_state 用于跟踪思考阶段，实现 ***思考开始***/***思考结束*** 包裹
        """
        if think_state is None:
            think_state = {"started": False, "ended": False}
        
        sse_lines = []
        
        for frame in kimi_frames:
            data = frame.get('data', {})
            flag = frame.get('flag', 0)
            mask = data.get('mask', '')
            
            # 跳过 heartbeat 和 EOF
            if 'heartbeat' in data or (flag == 2 and not data):
                continue
            
            # ======================
            # 新格式: block 内容
            # ======================
            if 'block' in data:
                block = data['block']
                
                # --- think 内容 ---
                if 'think' in mask and 'content' in mask:
                    content = ""
                    if isinstance(block, dict):
                        if 'think' in block and isinstance(block['think'], dict):
                            content = block['think'].get('content', '')
                        elif 'content' in block:
                            content = block.get('content', '')
                    
                    if content:
                        # 第一个 think chunk: 发送开始标记
                        if not think_state["started"]:
                            think_state["started"] = True
                            sse_lines.append(f"data: {json.dumps({
                                'id': completion_id, 'object': 'chat.completion.chunk',
                                'created': int(time.time()), 'model': model,
                                'choices': [{'index': 0, 'delta': {'content': '<thinking>\n'}, 'finish_reason': None}]
                            }, ensure_ascii=False)}\n\n")
                        
                        sse_lines.append(f"data: {json.dumps({
                            'id': completion_id, 'object': 'chat.completion.chunk',
                            'created': int(time.time()), 'model': model,
                            'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': None}]
                        }, ensure_ascii=False)}\n\n")
                    continue
                
                # --- text 内容 ---
                if 'text' in mask and 'content' in mask:
                    content = ""
                    if isinstance(block, dict):
                        if 'text' in block and isinstance(block['text'], dict):
                            content = block['text'].get('content', '')
                        elif 'content' in block:
                            content = block.get('content', '')
                    
                    if content:
                        # 第一个 text chunk 且 think 已开始但未结束: 发送结束标记
                        if think_state["started"] and not think_state["ended"]:
                            think_state["ended"] = True
                            sse_lines.append(f"data: {json.dumps({
                                'id': completion_id, 'object': 'chat.completion.chunk',
                                'created': int(time.time()), 'model': model,
                                'choices': [{'index': 0, 'delta': {'content': '\n</thinking>\n\n'}, 'finish_reason': None}]
                            }, ensure_ascii=False)}\n\n")
                        
                        sse_lines.append(f"data: {json.dumps({
                            'id': completion_id, 'object': 'chat.completion.chunk',
                            'created': int(time.time()), 'model': model,
                            'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': None}]
                        }, ensure_ascii=False)}\n\n")
                    continue
                
                # stage 标记，跳过
                if 'stage' in mask:
                    continue
                
                # 其他 block 尝试提取
                if isinstance(block, dict):
                    for key in ['content', 'text', 'think']:
                        if key in block:
                            val = block[key]
                            content = val if isinstance(val, str) else val.get('content', '') if isinstance(val, dict) else ''
                            if content:
                                sse_lines.append(f"data: {json.dumps({
                                    'id': completion_id, 'object': 'chat.completion.chunk',
                                    'created': int(time.time()), 'model': model,
                                    'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': None}]
                                }, ensure_ascii=False)}\n\n")
                            break
                continue
            
            # ======================
            # message 状态更新
            # ======================
            if 'message' in data:
                msg = data['message']
                role = msg.get('role', '')
                status = msg.get('status', '')
                
                if role == 'assistant' and status == 'MESSAGE_STATUS_GENERATING':
                    sse_lines.append(f"data: {json.dumps({
                        'id': completion_id, 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': model,
                        'choices': [{'index': 0, 'delta': {'role': 'assistant'}, 'finish_reason': None}]
                    }, ensure_ascii=False)}\n\n")
                elif status == 'MESSAGE_STATUS_COMPLETED':
                    # 如果 think 开始了但还没结束，在这里补结束标记
                    if think_state["started"] and not think_state["ended"]:
                        think_state["ended"] = True
                        sse_lines.append(f"data: {json.dumps({
                            'id': completion_id, 'object': 'chat.completion.chunk',
                            'created': int(time.time()), 'model': model,
                            'choices': [{'index': 0, 'delta': {'content': '\n</thinking>\n\n'}, 'finish_reason': None}]
                        }, ensure_ascii=False)}\n\n")
                    
                    sse_lines.append(f"data: {json.dumps({
                        'id': completion_id, 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': model,
                        'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]
                    }, ensure_ascii=False)}\n\n")
                continue
            
            # ======================
            # done 事件
            # ======================
            if 'done' in data:
                if think_state["started"] and not think_state["ended"]:
                    think_state["ended"] = True
                    sse_lines.append(f"data: {json.dumps({
                        'id': completion_id, 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': model,
                        'choices': [{'index': 0, 'delta': {'content': '\n</thinking>\n\n'}, 'finish_reason': None}]
                    }, ensure_ascii=False)}\n\n")
                
                sse_lines.append(f"data: {json.dumps({
                    'id': completion_id, 'object': 'chat.completion.chunk',
                    'created': int(time.time()), 'model': model,
                    'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]
                }, ensure_ascii=False)}\n\n")
                continue
            
            # ======================
            # 旧格式兼容
            # ======================
            if 'think' in data:
                content = data['think'].get('content', '')
                if content:
                    if not think_state["started"]:
                        think_state["started"] = True
                        sse_lines.append(f"data: {json.dumps({
                            'id': completion_id, 'object': 'chat.completion.chunk',
                            'created': int(time.time()), 'model': model,
                            'choices': [{'index': 0, 'delta': {'content': '</thinking>\n'}, 'finish_reason': None}]
                        }, ensure_ascii=False)}\n\n")
                    sse_lines.append(f"data: {json.dumps({
                        'id': completion_id, 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': model,
                        'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': None}]
                    }, ensure_ascii=False)}\n\n")
                continue
            
            if 'text' in data:
                content = data['text'].get('content', '')
                if content:
                    if think_state["started"] and not think_state["ended"]:
                        think_state["ended"] = True
                        sse_lines.append(f"data: {json.dumps({
                            'id': completion_id, 'object': 'chat.completion.chunk',
                            'created': int(time.time()), 'model': model,
                            'choices': [{'index': 0, 'delta': {'content': '\n</thinking>\n\n'}, 'finish_reason': None}]
                        }, ensure_ascii=False)}\n\n")
                    sse_lines.append(f"data: {json.dumps({
                        'id': completion_id, 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': model,
                        'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': None}]
                    }, ensure_ascii=False)}\n\n")
                continue
        
        return sse_lines


# =============================================================================
# Kimi 网页版 API 客户端
# =============================================================================

class KimiWebClient:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.kimi_base_url
        self.headers = self._build_headers()
        self._last_chat_id: Optional[str] = None
    
    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/connect+json",
            "Accept": "application/connect+json",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": "https://www.kimi.com",
            "Referer": "https://www.kimi.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-msh-platform": "web",
            "x-msh-device-id": "7567777970924893952"
        }
        if self.config.access_token:
            headers["Authorization"] = f"Bearer {self.config.access_token}"
        return headers
    
    async def chat_stream(self, messages: List[Dict], model: str) -> AsyncGenerator[List[Dict], None]:
        url = f"{self.base_url}/apiv2/kimi.gateway.chat.v1.ChatService/Chat"
        body = ProtocolConverter.openai_to_kimi_request(messages, self.config)
        self._last_chat_id = None
        
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            async with client.stream("POST", url, content=body, headers=self.headers) as response:
                if response.status_code != 200:
                    error_text = await response.aread()
                    raise HTTPException(status_code=response.status_code, detail=f"Kimi API error: {error_text[:500]}")
                
                buffer = b""
                async for chunk in response.aiter_bytes():
                    buffer += chunk
                    frames, consumed = ConnectCodec.decode_frames(buffer)
                    if frames:
                        buffer = buffer[consumed:]
                        # 提取 chat_id
                        for frame in frames:
                            data = frame.get('data', {})
                            chat = data.get('chat', {})
                            if chat and 'id' in chat:
                                self._last_chat_id = chat['id']
                        yield frames
                
                # 处理剩余 buffer
                if buffer:
                    frames, consumed = ConnectCodec.decode_frames(buffer)
                    if frames:
                        for frame in frames:
                            data = frame.get('data', {})
                            chat = data.get('chat', {})
                            if chat and 'id' in chat:
                                self._last_chat_id = chat['id']
                        yield frames
    
    async def delete_chat(self, chat_id: Optional[str] = None):
        """删除指定会话，防止会话列表爆炸
        
        逆向发现 (2026-07-27):
        - 端点: POST https://www.kimi.com/apiv2/kimi.chat.v1.ChatService/DeleteChat
        - Content-Type: application/json (不是 connect+json!)
        - 请求体: {"chatId": "..."}
        """
        cid = chat_id or self._last_chat_id
        if not cid:
            return
        
        url = f"{self.base_url}/apiv2/kimi.chat.v1.ChatService/DeleteChat"
        
        # 删除请求用 application/json，不是 connect+json
        delete_headers = dict(self.headers)
        delete_headers["Content-Type"] = "application/json"
        delete_headers["Accept"] = "application/json"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    url,
                    json={"chatId": cid},
                    headers=delete_headers
                )
                if response.status_code == 200:
                    print(f"[auto-delete] 会话已删除: {cid[:20]}...")
                    return
                else:
                    print(f"[auto-delete] 删除失败 HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            print(f"[auto-delete] 删除异常: {e}")
        """删除指定会话，防止会话列表爆炸"""
        cid = chat_id or self._last_chat_id
        if not cid:
            return
        
        # 尝试几种可能的删除端点
        delete_endpoints = [
            "/apiv2/kimi.gateway.chat.v1.ChatService/DeleteChat",
            "/apiv2/kimi.gateway.chat.v1.ChatService/ClearChat",
            "/apiv2/kimi.gateway.history.v1.HistoryService/DeleteChat",
        ]
        
        for endpoint in delete_endpoints:
            url = f"{self.base_url}{endpoint}"
            body = ConnectCodec.encode_frame({
                "scenario": self.config.scenario,
                "chat": {"id": cid}
            })
            
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.post(url, content=body, headers=self.headers)
                    if response.status_code == 200:
                        print(f"[auto-delete] 会话已删除: {cid[:20]}...")
                        return
            except Exception:
                continue
        
        print(f"[auto-delete] 删除会话失败: {cid[:20]}... (尝试 {len(delete_endpoints)} 个端点)")


# =============================================================================
# FastAPI 应用
# =============================================================================

def create_app(config: Config) -> FastAPI:
    app = FastAPI(title="Kimi2OpenAI-Web", version="2.2.0")
    
    @app.get("/health")
    async def health():
        return {"status": "ok", "mode": "web-reverse-v2.2", "token_ok": config.access_token is not None}
    
    @app.get("/v1/models")
    async def list_models():
        return {"object": "list", "data": [
            {"id": "kimi-k3-web", "object": "model", "created": 1700000000, "owned_by": "moonshot-ai-web"},
            {"id": "kimi-k2.6-web", "object": "model", "created": 1700000000, "owned_by": "moonshot-ai-web"},
        ]}
    
    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        try:
            body = await request.json()
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")
        
        messages = body.get("messages", [])
        model = body.get("model", "kimi-k3-web")
        stream = body.get("stream", False)
        
        if not config.access_token:
            raise HTTPException(status_code=500, detail="KIMI_ACCESS_TOKEN not set")
        
        client = KimiWebClient(config)
        completion_id = f"kimi-web-{uuid.uuid4().hex[:12]}"
        
        if stream:
            think_state = {"started": False, "ended": False}
            
            async def stream_generator():
                try:
                    async for frames in client.chat_stream(messages, model):
                        for line in ProtocolConverter.kimi_to_openai_stream(frames, model, completion_id, think_state):
                            yield line
                    yield "data: [DONE]\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'error': str(e)})}\n\ndata: [DONE]\n\n"
                finally:
                    if config.auto_delete_chat:
                        await client.delete_chat()
            
            return StreamingResponse(stream_generator(), media_type="text/event-stream")
        else:
            full_content = ""
            think_content = ""
            in_think = False
            think_started = False
            
            try:
                async for frames in client.chat_stream(messages, model):
                    for frame in frames:
                        data = frame.get('data', {})
                        mask = data.get('mask', '')
                        block = data.get('block', {})
                        
                        if 'block' in data and isinstance(block, dict):
                            # think 内容
                            if 'think' in mask and 'content' in mask:
                                content = ""
                                if 'think' in block and isinstance(block['think'], dict):
                                    content = block['think'].get('content', '')
                                elif 'content' in block:
                                    content = block.get('content', '')
                                if content:
                                    if not think_started:
                                        think_started = True
                                        in_think = True
                                    think_content += content
                            
                            # text 内容
                            elif 'text' in mask and 'content' in mask:
                                content = ""
                                if 'text' in block and isinstance(block['text'], dict):
                                    content = block['text'].get('content', '')
                                elif 'content' in block:
                                    content = block.get('content', '')
                                if content:
                                    if in_think:
                                        in_think = False
                                    full_content += content
                        
                        # 旧格式
                        elif 'think' in data:
                            content = data['think'].get('content', '')
                            if content:
                                if not think_started:
                                    think_started = True
                                    in_think = True
                                think_content += content
                        elif 'text' in data:
                            content = data['text'].get('content', '')
                            if content:
                                if in_think:
                                    in_think = False
                                full_content += content
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Proxy error: {str(e)}")
            finally:
                if config.auto_delete_chat:
                    await client.delete_chat()
            
            # 组装最终内容：思考 + 回复
            final_content = ""
            if think_content:
                final_content += f"<thinking>\n{think_content}\n</thinking>\n\n"
            final_content += full_content
            
            return JSONResponse({
                "id": completion_id, "object": "chat.completion",
                "created": int(time.time()), "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": final_content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            })
    
    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--access-token")
    parser.add_argument("--scenario", default="SCENARIO_K2D5")
    parser.add_argument("--no-delete", action="store_true", help="禁用自动删除会话")
    args = parser.parse_args()
    
    config = Config(
        host=args.host, port=args.port,
        access_token=args.access_token or os.environ.get("KIMI_ACCESS_TOKEN"),
        scenario=args.scenario,
        auto_delete_chat=not args.no_delete
    )
    
    print("🌙 Kimi2OpenAI-Web v2.2")
    print(f"   URL: http://{config.host}:{config.port}")
    print(f"   Token: {'✅' if config.access_token else '❌ 未配置'}")
    print(f"   自动删除: {'✅' if config.auto_delete_chat else '❌'}")
    if not config.access_token:
        print("   获取: 登录 kimi.com → F12 → Local Storage → access_token")
    
    uvicorn.run(create_app(config), host=config.host, port=config.port, log_level="info")


if __name__ == "__main__":
    main()
