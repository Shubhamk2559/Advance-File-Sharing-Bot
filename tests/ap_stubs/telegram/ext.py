class ContextTypes: DEFAULT_TYPE = object
class Application: pass
class ApplicationHandlerStop(Exception): pass
class _F:
    def __init__(self, name="f"): self.name = name
    def __and__(self, o): return _F(f"({self.name}&{getattr(o,'name',o)})")
    __or__ = __rand__ = __ror__ = __and__
    def __invert__(self): return _F(f"~{self.name}")
    def __getattr__(self, n):
        if n.startswith("__"): raise AttributeError(n)
        return _F(f"{self.name}.{n}")
    def __call__(self, *a, **k): return _F(f"{self.name}()")
filters = _F("filters")
class _H:
    def __init__(self, *a, **k): self.args, self.kw = a, k
class CallbackQueryHandler(_H): pass
class CommandHandler(_H): pass
class MessageHandler(_H): pass
class ChatMemberHandler(_H):
    MY_CHAT_MEMBER = 1
class ChatJoinRequestHandler(_H): pass
