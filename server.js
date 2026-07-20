'use strict';
// oxy chess relay
// - POST /push { token, fen }  : in-game client pushes the current position
// - GET  /view/:token          : private live board viewer (open on your phone / 2nd screen)
// - GET  /state/:token         : JSON snapshot (polling fallback)
// - WS   /ws?token=...         : live push to the viewer
// Stockfish runs natively (see Dockerfile). Sessions are keyed by an unguessable token.

const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');

// viewer page, inlined (base64) so the service is self-contained
const VIEW_HTML = Buffer.from('PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImVuIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04IiAvPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLCBpbml0aWFsLXNjYWxlPTEsIG1heGltdW0tc2NhbGU9MSwgdXNlci1zY2FsYWJsZT1ubyIgLz4KPG1ldGEgbmFtZT0idGhlbWUtY29sb3IiIGNvbnRlbnQ9IiMwYjBlMTQiIC8+Cjx0aXRsZT5veHkgY2hlc3M8L3RpdGxlPgo8c3R5bGU+CiAgOnJvb3QgeyAtLWx0OiNiOWMyZDA7IC0tZGs6IzVmNmI3ZDsgLS1mcm9tOiNlMGIzNGQ7IC0tdG86IzhmZDY2YTsgLS1iZzojMGIwZTE0OyAtLWNhcmQ6IzE1MWEyNDsgLS10eDojZThlZGY0OyAtLW11dDojOGE5NGE2OyAtLWFjYzojN2Q1NWZmOyB9CiAgKiB7IGJveC1zaXppbmc6Ym9yZGVyLWJveDsgLXdlYmtpdC10YXAtaGlnaGxpZ2h0LWNvbG9yOnRyYW5zcGFyZW50OyB9CiAgaHRtbCxib2R5IHsgbWFyZ2luOjA7IGJhY2tncm91bmQ6dmFyKC0tYmcpOyBjb2xvcjp2YXIoLS10eCk7IGZvbnQ6MTVweC8xLjQgLWFwcGxlLXN5c3RlbSxTZWdvZSBVSSxSb2JvdG8sc2Fucy1zZXJpZjsgfQogIC53cmFwIHsgbWF4LXdpZHRoOjUyMHB4OyBtYXJnaW46MCBhdXRvOyBwYWRkaW5nOjE0cHg7IH0KICAudG9wIHsgZGlzcGxheTpmbGV4OyBhbGlnbi1pdGVtczpjZW50ZXI7IGdhcDoxMHB4OyBtYXJnaW4tYm90dG9tOjEycHg7IH0KICAuYnJhbmQgeyBmb250LXdlaWdodDo3MDA7IGxldHRlci1zcGFjaW5nOi41cHg7IH0KICAuZG90IHsgd2lkdGg6OHB4OyBoZWlnaHQ6OHB4OyBib3JkZXItcmFkaXVzOjUwJTsgYmFja2dyb3VuZDojZTA1NTRkOyBib3gtc2hhZG93OjAgMCA4cHggI2UwNTU0ZDsgfQogIC5kb3Qub24geyBiYWNrZ3JvdW5kOiM1YWQwN2E7IGJveC1zaGFkb3c6MCAwIDhweCAjNWFkMDdhOyB9CiAgLmJvYXJkYm94IHsgcG9zaXRpb246cmVsYXRpdmU7IHdpZHRoOjEwMCU7IGFzcGVjdC1yYXRpbzoxLzE7IGJvcmRlci1yYWRpdXM6MTJweDsgb3ZlcmZsb3c6aGlkZGVuOyBib3gtc2hhZG93OjAgMTBweCA0MHB4IHJnYmEoMCwwLDAsLjUpOyB9CiAgI2JvYXJkIHsgZGlzcGxheTpncmlkOyBncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDgsMWZyKTsgZ3JpZC10ZW1wbGF0ZS1yb3dzOnJlcGVhdCg4LDFmcik7IHdpZHRoOjEwMCU7IGhlaWdodDoxMDAlOyB9CiAgLnNxIHsgcG9zaXRpb246cmVsYXRpdmU7IGRpc3BsYXk6ZmxleDsgYWxpZ24taXRlbXM6Y2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyOyBmb250LXNpemU6bWluKDl2dyw0NnB4KTsgbGluZS1oZWlnaHQ6MTsgdXNlci1zZWxlY3Q6bm9uZTsgfQogIC5zcS5sIHsgYmFja2dyb3VuZDp2YXIoLS1sdCk7IH0gLnNxLmQgeyBiYWNrZ3JvdW5kOnZhcigtLWRrKTsgfQogIC5zcS5obGZyb206OmFmdGVyLC5zcS5obHRvOjphZnRlciB7IGNvbnRlbnQ6IiI7IHBvc2l0aW9uOmFic29sdXRlOyBpbnNldDowOyB9CiAgLnNxLmhsZnJvbTo6YWZ0ZXIgeyBiYWNrZ3JvdW5kOnZhcigtLWZyb20pOyBvcGFjaXR5Oi40MjsgfQogIC5zcS5obHRvOjphZnRlciB7IGJhY2tncm91bmQ6dmFyKC0tdG8pOyBvcGFjaXR5Oi40MjsgfQogIC5wYyB7IHBvc2l0aW9uOnJlbGF0aXZlOyB6LWluZGV4OjE7IGZpbHRlcjpkcm9wLXNoYWRvdygwIDFweCAxcHggcmdiYSgwLDAsMCwuNDUpKTsgfQogIC5wYy53IHsgY29sb3I6I2ZiZmRmZjsgfSAucGMuYiB7IGNvbG9yOiMyMDI2MmY7IH0KICAjYXJyb3cgeyBwb3NpdGlvbjphYnNvbHV0ZTsgaW5zZXQ6MDsgd2lkdGg6MTAwJTsgaGVpZ2h0OjEwMCU7IHBvaW50ZXItZXZlbnRzOm5vbmU7IHotaW5kZXg6MjsgfQogIC5pbmZvIHsgZGlzcGxheTpncmlkOyBncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcjsgZ2FwOjEwcHg7IG1hcmdpbi10b3A6MTJweDsgfQogIC5jYXJkIHsgYmFja2dyb3VuZDp2YXIoLS1jYXJkKTsgYm9yZGVyLXJhZGl1czoxMnB4OyBwYWRkaW5nOjEycHggMTRweDsgfQogIC5sYmwgeyBjb2xvcjp2YXIoLS1tdXQpOyBmb250LXNpemU6MTJweDsgdGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlOyBsZXR0ZXItc3BhY2luZzouNnB4OyB9CiAgLmJpZyB7IGZvbnQtc2l6ZToyNnB4OyBmb250LXdlaWdodDo3MDA7IG1hcmdpbi10b3A6MnB4OyB9CiAgLmJlc3QgeyBjb2xvcjp2YXIoLS10byk7IH0gLmV2YWxwIHsgY29sb3I6IzlmZDBmZjsgfSAuZXZhbG4geyBjb2xvcjojZmY5YTlhOyB9CiAgLnJvdyB7IGRpc3BsYXk6ZmxleDsgYWxpZ24taXRlbXM6Y2VudGVyOyBnYXA6OHB4OyBtYXJnaW4tdG9wOjEycHg7IGp1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuOyB9CiAgYnV0dG9uIHsgYmFja2dyb3VuZDp2YXIoLS1jYXJkKTsgY29sb3I6dmFyKC0tdHgpOyBib3JkZXI6MXB4IHNvbGlkICMyNjMwM2Y7IGJvcmRlci1yYWRpdXM6OXB4OyBwYWRkaW5nOjhweCAxMnB4OyBmb250LXNpemU6MTNweDsgfQogIGJ1dHRvbjphY3RpdmUgeyBiYWNrZ3JvdW5kOiMxZDI0MzE7IH0KICAuZXZhbGJhciB7IGhlaWdodDo2cHg7IGJvcmRlci1yYWRpdXM6NnB4OyBiYWNrZ3JvdW5kOiMyNjMwM2Y7IG92ZXJmbG93OmhpZGRlbjsgbWFyZ2luLXRvcDoxMHB4OyB9CiAgLmV2YWxiYXIgPiBpIHsgZGlzcGxheTpibG9jazsgaGVpZ2h0OjEwMCU7IHdpZHRoOjUwJTsgYmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoOTBkZWcsIzVhZDA3YSwjOWZkMGZmKTsgdHJhbnNpdGlvbjp3aWR0aCAuM3M7IH0KICAubXV0ZWQgeyBjb2xvcjp2YXIoLS1tdXQpOyBmb250LXNpemU6MTJweDsgfQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8ZGl2IGNsYXNzPSJ3cmFwIj4KICA8ZGl2IGNsYXNzPSJ0b3AiPjxzcGFuIGNsYXNzPSJkb3QiIGlkPSJkb3QiPjwvc3Bhbj48c3BhbiBjbGFzcz0iYnJhbmQiPm94eSBjaGVzczwvc3Bhbj48c3BhbiBjbGFzcz0ibXV0ZWQiIGlkPSJ0dXJuIj48L3NwYW4+PC9kaXY+CiAgPGRpdiBjbGFzcz0iYm9hcmRib3giPgogICAgPGRpdiBpZD0iYm9hcmQiPjwvZGl2PgogICAgPHN2ZyBpZD0iYXJyb3ciIHZpZXdCb3g9IjAgMCA4IDgiIHByZXNlcnZlQXNwZWN0UmF0aW89Im5vbmUiPgogICAgICA8ZGVmcz48bWFya2VyIGlkPSJhaCIgbWFya2VyV2lkdGg9IjQiIG1hcmtlckhlaWdodD0iNCIgcmVmWD0iMi40IiByZWZZPSIyIiBvcmllbnQ9ImF1dG8iPgogICAgICAgIDxwYXRoIGQ9Ik0wLDAgTDQsMiBMMCw0IHoiIGZpbGw9IiM4ZmQ2NmEiLz48L21hcmtlcj48L2RlZnM+CiAgICAgIDxsaW5lIGlkPSJhbGluZSIgeDE9IjAiIHkxPSIwIiB4Mj0iMCIgeTI9IjAiIHN0cm9rZT0iIzhmZDY2YSIgc3Ryb2tlLXdpZHRoPSIwLjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiIG1hcmtlci1lbmQ9InVybCgjYWgpIiBvcGFjaXR5PSIwIi8+CiAgICA8L3N2Zz4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJldmFsYmFyIj48aSBpZD0iZWJhciI+PC9pPjwvZGl2PgogIDxkaXYgY2xhc3M9ImluZm8iPgogICAgPGRpdiBjbGFzcz0iY2FyZCI+PGRpdiBjbGFzcz0ibGJsIj5CZXN0IG1vdmU8L2Rpdj48ZGl2IGNsYXNzPSJiaWcgYmVzdCIgaWQ9ImJlc3QiPuKAlDwvZGl2PjwvZGl2PgogICAgPGRpdiBjbGFzcz0iY2FyZCI+PGRpdiBjbGFzcz0ibGJsIj5FdmFsPC9kaXY+PGRpdiBjbGFzcz0iYmlnIiBpZD0iZXZhbCI+4oCUPC9kaXY+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0icm93Ij48c3BhbiBjbGFzcz0ibXV0ZWQiIGlkPSJwdiI+d2FpdGluZyBmb3IgdGhlIGJvYXJk4oCmPC9zcGFuPjxidXR0b24gaWQ9ImZsaXAiPkZsaXA8L2J1dHRvbj48L2Rpdj4KPC9kaXY+CjxzY3JpcHQ+CmNvbnN0IFRPS0VOID0gZGVjb2RlVVJJQ29tcG9uZW50KGxvY2F0aW9uLnBhdGhuYW1lLnNwbGl0KCcvJykucG9wKCkgfHwgJycpOwpjb25zdCBVTkkgPSB7IFA6J+KZmScsTjon4pmYJyxCOifimZcnLFI6J+KZlicsUTon4pmVJyxLOifimZQnLCBwOifimZ8nLG46J+KZnicsYjon4pmdJyxyOifimZwnLHE6J+KZmycsazon4pmaJyB9OwpsZXQgZmxpcHBlZCA9IGZhbHNlLCBsYXN0ID0gbnVsbDsKCmZ1bmN0aW9uIHBhcnNlRkVOKGZlbil7CiAgY29uc3QgW3BsYWNlbWVudCwgc2lkZV0gPSBmZW4uc3BsaXQoJyAnKTsKICBjb25zdCByb3dzID0gcGxhY2VtZW50LnNwbGl0KCcvJyk7IC8vIHJhbmsgOCAtPiByYW5rIDEKICBjb25zdCBzcSA9IHt9OyAvLyAiZTQiIC0+IHBpZWNlIGxldHRlcgogIGZvciAobGV0IHI9MDsgcjw4OyByKyspewogICAgbGV0IGY9MDsKICAgIGZvciAoY29uc3QgY2ggb2Ygcm93c1tyXSl7CiAgICAgIGlmICgvXGQvLnRlc3QoY2gpKXsgZiArPSArY2g7IH0KICAgICAgZWxzZSB7IGNvbnN0IGZpbGU9ImFiY2RlZmdoIltmXTsgY29uc3QgcmFuaz04LXI7IHNxW2ZpbGUrcmFua109Y2g7IGYrKzsgfQogICAgfQogIH0KICByZXR1cm4geyBzcSwgc2lkZSB9Owp9CmZ1bmN0aW9uIHJlbmRlcihyZWMpewogIGNvbnN0IGJvYXJkID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2JvYXJkJyk7CiAgY29uc3QgYWxpbmUgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnYWxpbmUnKTsKICBpZiAoIXJlYyB8fCAhcmVjLmZlbil7IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdwdicpLnRleHRDb250ZW50PSd3YWl0aW5nIGZvciB0aGUgYm9hcmTigKYnOyByZXR1cm47IH0KICBjb25zdCB7IHNxLCBzaWRlIH0gPSBwYXJzZUZFTihyZWMuZmVuKTsKICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHVybicpLnRleHRDb250ZW50ID0gKHNpZGU9PT0ndyc/J1doaXRlJzonQmxhY2snKSsnIHRvIG1vdmUnOwogIGNvbnN0IGZpbGVzID0gZmxpcHBlZCA/IFsuLi4naGdmZWRjYmEnXSA6IFsuLi4nYWJjZGVmZ2gnXTsKICBjb25zdCByYW5rcyA9IGZsaXBwZWQgPyBbMSwyLDMsNCw1LDYsNyw4XSA6IFs4LDcsNiw1LDQsMywyLDFdOwogIGJvYXJkLmlubmVySFRNTD0nJzsKICBjb25zdCBmcm9tID0gcmVjLmJlc3Rtb3ZlID8gcmVjLmJlc3Rtb3ZlLnNsaWNlKDAsMikgOiBudWxsOwogIGNvbnN0IHRvICAgPSByZWMuYmVzdG1vdmUgPyByZWMuYmVzdG1vdmUuc2xpY2UoMiw0KSA6IG51bGw7CiAgcmFua3MuZm9yRWFjaCgocmFuayk9PmZpbGVzLmZvckVhY2goKGZpbGUpPT57CiAgICBjb25zdCBuYW1lPWZpbGUrcmFuazsKICAgIGNvbnN0IGRhcms9KCAoImFiY2RlZmdoIi5pbmRleE9mKGZpbGUpK3JhbmspICUgMiA9PT0wICk7CiAgICBjb25zdCBkPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2RpdicpOwogICAgZC5jbGFzc05hbWU9J3NxICcrKGRhcms/J2QnOidsJykrKG5hbWU9PT1mcm9tPycgaGxmcm9tJzonJykrKG5hbWU9PT10bz8nIGhsdG8nOicnKTsKICAgIGNvbnN0IHA9c3FbbmFtZV07CiAgICBpZihwKXsgY29uc3Qgcz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdzcGFuJyk7IHMuY2xhc3NOYW1lPSdwYyAnKyhwPT09cC50b1VwcGVyQ2FzZSgpPyd3JzonYicpOyBzLnRleHRDb250ZW50PVVOSVtwXTsgZC5hcHBlbmRDaGlsZChzKTsgfQogICAgYm9hcmQuYXBwZW5kQ2hpbGQoZCk7CiAgfSkpOwogIC8vIGFycm93IChncmlkIGNvb3JkcyAwLi44LCBjZW50ZXJzIGF0ICswLjUpCiAgaWYgKGZyb20gJiYgdG8pewogICAgY29uc3QgY3g9KHNxTmFtZSk9PnsgY29uc3QgZmk9ImFiY2RlZmdoIi5pbmRleE9mKHNxTmFtZVswXSk7IGNvbnN0IHg9ZmxpcHBlZD83LWZpOmZpOyByZXR1cm4geCswLjU7IH07CiAgICBjb25zdCBjeT0oc3FOYW1lKT0+eyBjb25zdCByYT0rc3FOYW1lWzFdOyBjb25zdCB5PWZsaXBwZWQ/cmEtMTo4LXJhOyByZXR1cm4geSswLjU7IH07CiAgICBhbGluZS5zZXRBdHRyaWJ1dGUoJ3gxJyxjeChmcm9tKSk7IGFsaW5lLnNldEF0dHJpYnV0ZSgneTEnLGN5KGZyb20pKTsKICAgIGFsaW5lLnNldEF0dHJpYnV0ZSgneDInLGN4KHRvKSk7ICAgYWxpbmUuc2V0QXR0cmlidXRlKCd5MicsY3kodG8pKTsKICAgIGFsaW5lLnNldEF0dHJpYnV0ZSgnb3BhY2l0eScsJzAuOTUnKTsKICB9IGVsc2UgYWxpbmUuc2V0QXR0cmlidXRlKCdvcGFjaXR5JywnMCcpOwogIC8vIGV2YWwgKHdoaXRlIFBPVikKICBjb25zdCBiZXN0RWw9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2Jlc3QnKSwgZXZFbD1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZXZhbCcpLCBiYXI9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2ViYXInKTsKICBiZXN0RWwudGV4dENvbnRlbnQgPSByZWMuYmVzdG1vdmUgPyAoZnJvbSsn4oaSJyt0bykgOiAocmVjLmZlbj8gJ+KApnRoaW5raW5nJyA6ICfigJQnKTsKICBsZXQgZXZUZXh0PSfigJQnLCBmcmFjPTAuNSwgY2xzPScnOwogIGlmIChyZWMubWF0ZSE9bnVsbCl7IGNvbnN0IG0gPSBzaWRlPT09J2InPy1yZWMubWF0ZTpyZWMubWF0ZTsgZXZUZXh0PShtPjA/JyMnOicjLScpK01hdGguYWJzKG0pOyBmcmFjPW0+MD8xOjA7IGNscz1tPjA/J2V2YWxwJzonZXZhbG4nOyB9CiAgZWxzZSBpZiAocmVjLmNwIT1udWxsKXsgY29uc3QgY3A9KHNpZGU9PT0nYic/LXJlYy5jcDpyZWMuY3ApLzEwMDsgZXZUZXh0PShjcD4wPycrJzonJykrY3AudG9GaXhlZCgyKTsgZnJhYz0xLygxK01hdGguZXhwKC1jcC8zKSk7IGNscz1jcD49MD8nZXZhbHAnOidldmFsbic7IH0KICBldkVsLnRleHRDb250ZW50PWV2VGV4dDsgZXZFbC5jbGFzc05hbWU9J2JpZyAnK2NsczsKICBiYXIuc3R5bGUud2lkdGg9TWF0aC5yb3VuZChmcmFjKjEwMCkrJyUnOwogIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdwdicpLnRleHRDb250ZW50ID0gcmVjLnB2ID8gKCdwdjogJytyZWMucHYuam9pbignICcpKSA6IChyZWMuYmVzdG1vdmU/Jyc6J2FuYWx5emluZ+KApicpOwp9CmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdmbGlwJykub25jbGljaz0oKT0+eyBmbGlwcGVkPSFmbGlwcGVkOyBpZihsYXN0KSByZW5kZXIobGFzdCk7IH07CgovLyBsaXZlIGNvbm5lY3Rpb246IFdTLCBwb2xsaW5nIGZhbGxiYWNrCmNvbnN0IGRvdD1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZG90Jyk7CmZ1bmN0aW9uIGNvbm5lY3QoKXsKICBjb25zdCBwcm90bz1sb2NhdGlvbi5wcm90b2NvbD09PSdodHRwczonPyd3c3MnOid3cyc7CiAgY29uc3Qgd3M9bmV3IFdlYlNvY2tldChwcm90bysnOi8vJytsb2NhdGlvbi5ob3N0Kycvd3M/dG9rZW49JytlbmNvZGVVUklDb21wb25lbnQoVE9LRU4pKTsKICB3cy5vbm9wZW49KCk9PmRvdC5jbGFzc0xpc3QuYWRkKCdvbicpOwogIHdzLm9uY2xvc2U9KCk9PnsgZG90LmNsYXNzTGlzdC5yZW1vdmUoJ29uJyk7IHNldFRpbWVvdXQoY29ubmVjdCwxNTAwKTsgfTsKICB3cy5vbmVycm9yPSgpPT57IHRyeXt3cy5jbG9zZSgpO31jYXRjaChlKXt9IH07CiAgd3Mub25tZXNzYWdlPShlKT0+eyB0cnl7IGxhc3Q9SlNPTi5wYXJzZShlLmRhdGEpOyByZW5kZXIobGFzdCk7IH1jYXRjaChfKXt9IH07Cn0KY29ubmVjdCgpOwovLyBwb2xsaW5nIGJhY2t1cCBldmVyeSAycyBpbiBjYXNlIFdTIGlzIGJsb2NrZWQKc2V0SW50ZXJ2YWwoYXN5bmMoKT0+eyBpZihkb3QuY2xhc3NMaXN0LmNvbnRhaW5zKCdvbicpKXJldHVybjsgdHJ5eyBjb25zdCByPWF3YWl0IGZldGNoKCcvc3RhdGUvJytlbmNvZGVVUklDb21wb25lbnQoVE9LRU4pKTsgY29uc3Qgaj1hd2FpdCByLmpzb24oKTsgaWYoaiYmai5mZW4peyBsYXN0PWo7IHJlbmRlcihqKTt9IH1jYXRjaChfKXt9IH0sIDIwMDApOwo8L3NjcmlwdD4KPC9ib2R5Pgo8L2h0bWw+Cg==', 'base64').toString('utf8');

const PORT = process.env.PORT || 8080;
const STOCKFISH_PATH = process.env.STOCKFISH_PATH || 'stockfish';
const DEPTH = parseInt(process.env.SF_DEPTH || '16', 10);
const POOL_SIZE = parseInt(process.env.SF_POOL || '2', 10);
const SESSION_TTL_MS = 1000 * 60 * 60 * 6; // drop idle sessions after 6h

// ---------------------------------------------------------------- Stockfish
class Engine {
  constructor() {
    this.busy = false;
    this.buf = '';
    this.onLine = null;
    this._spawn();
  }
  _spawn() {
    this.proc = spawn(STOCKFISH_PATH, [], { stdio: ['pipe', 'pipe', 'ignore'] });
    this.proc.stdout.on('data', (d) => {
      this.buf += d.toString();
      let i;
      while ((i = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, i).trim();
        this.buf = this.buf.slice(i + 1);
        if (this.onLine) this.onLine(line);
      }
    });
    this.proc.on('exit', () => { setTimeout(() => this._spawn(), 500); });
    this.send('uci');
    this.send('setoption name Threads value 1');
    this.send('isready');
  }
  send(cmd) { try { this.proc.stdin.write(cmd + '\n'); } catch (_) {} }
  analyze(fen, depth) {
    return new Promise((resolve) => {
      let cp = null, mate = null, pv = null, done = false;
      const finish = (bestmove) => {
        if (done) return; done = true; this.onLine = null;
        resolve({ bestmove, cp, mate, pv });
      };
      const timer = setTimeout(() => finish(null), 8000);
      this.onLine = (line) => {
        if (line.startsWith('info') && line.includes(' pv ')) {
          const c = line.match(/score cp (-?\d+)/);
          const m = line.match(/score mate (-?\d+)/);
          if (m) { mate = parseInt(m[1], 10); cp = null; }
          else if (c) { cp = parseInt(c[1], 10); mate = null; }
          const p = line.match(/ pv (.+)$/);
          if (p) pv = p[1].split(' ').slice(0, 12);
        } else if (line.startsWith('bestmove')) {
          clearTimeout(timer);
          finish((line.split(/\s+/)[1] || '').replace(/[^a-h1-8qrbnQRBN]/g, '') || null);
        }
      };
      this.send('position fen ' + fen);
      this.send('go depth ' + depth);
    });
  }
}

const pool = [];
const waiters = [];
for (let i = 0; i < POOL_SIZE; i++) pool.push(new Engine());
function acquire() {
  return new Promise((resolve) => {
    const e = pool.find((x) => !x.busy);
    if (e) { e.busy = true; resolve(e); } else waiters.push(resolve);
  });
}
function release(e) {
  const w = waiters.shift();
  if (w) { w(e); } else { e.busy = false; }
}
async function analyze(fen, depth) {
  const e = await acquire();
  try { return await e.analyze(fen, depth); }
  finally { release(e); }
}

// ---------------------------------------------------------------- sessions
const sessions = new Map(); // token -> { fen, bestmove, cp, mate, pv, updatedAt }
const viewers = new Map();  // token -> Set<ws>

setInterval(() => {
  const now = Date.now();
  for (const [t, rec] of sessions) if (now - rec.updatedAt > SESSION_TTL_MS && !(viewers.get(t) || {}).size) sessions.delete(t);
}, 60000);

function validFen(f) {
  if (typeof f !== 'string' || f.length > 100) return false;
  return /^([pnbrqkPNBRQK1-8]+\/){7}[pnbrqkPNBRQK1-8]+ [wb] (-|[KQkq]+) (-|[a-h][36]) \d+ \d+$/.test(f)
      || /^([pnbrqkPNBRQK1-8]+\/){7}[pnbrqkPNBRQK1-8]+ [wb] /.test(f); // lenient tail
}
function validToken(t) { return typeof t === 'string' && /^[A-Za-z0-9_-]{6,64}$/.test(t); }

function broadcast(token, rec) {
  const set = viewers.get(token);
  if (!set) return;
  const msg = JSON.stringify(rec);
  for (const ws of set) { try { ws.send(msg); } catch (_) {} }
}

// ---------------------------------------------------------------- http
const app = express();
app.use(express.json({ limit: '16kb' }));

app.get('/', (_req, res) => res.type('text').send('oxy chess relay up'));

app.post('/push', async (req, res) => {
  const { token, fen } = req.body || {};
  if (!validToken(token) || !validFen(fen)) return res.status(400).json({ error: 'bad token/fen' });
  const prev = sessions.get(token);
  if (prev && prev.fen === fen && prev.bestmove) return res.json({ ok: true, cached: true });
  // record the raw fen immediately so the viewer flips instantly; analysis fills in
  const rec = { fen, bestmove: null, cp: null, mate: null, pv: null, updatedAt: Date.now() };
  sessions.set(token, rec);
  broadcast(token, rec);
  res.json({ ok: true });
  try {
    const r = await analyze(fen, DEPTH);
    const cur = sessions.get(token);
    if (!cur || cur.fen !== fen) return; // position moved on; drop stale result
    Object.assign(cur, r, { depth: DEPTH, updatedAt: Date.now() });
    broadcast(token, cur);
  } catch (_) {}
});

app.get('/state/:token', (req, res) => {
  const rec = sessions.get(req.params.token);
  res.json(rec || { waiting: true });
});

app.get('/view/:token', (req, res) => {
  if (!validToken(req.params.token)) return res.status(400).send('bad token');
  res.type('html').send(VIEW_HTML);
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  let token = null;
  try { token = new URL(req.url, 'http://x').searchParams.get('token'); } catch (_) {}
  if (!validToken(token)) { ws.close(); return; }
  if (!viewers.has(token)) viewers.set(token, new Set());
  viewers.get(token).add(ws);
  const rec = sessions.get(token);
  if (rec) { try { ws.send(JSON.stringify(rec)); } catch (_) {} }
  ws.on('close', () => { const s = viewers.get(token); if (s) s.delete(ws); });
  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('oxy chess relay listening on ' + PORT + ' (depth ' + DEPTH + ', pool ' + POOL_SIZE + ')'));
