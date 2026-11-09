"""Auto-poster: schedules posts (shuffle / loop / one-pass) into channels and groups.

Merged into the file-sharing bot: it shares the bot's token, owner and MongoDB connection (collections are all prefixed `ap_`),
runs as supervised asyncio loops (no JobQueue / extra dependency), and adds "Channel -> file posts" (importer.py).
"""

__version__ = "2.1.0"
