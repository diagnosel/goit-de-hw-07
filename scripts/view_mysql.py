"""Read-only local viewer for the live homework result table."""

import html
from http.server import BaseHTTPRequestHandler, HTTPServer

import mysql.connector


class Viewer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/":
            self.send_error(404)
            return
        connection = mysql.connector.connect(host="127.0.0.1", port=13307, user="root")
        try:
            cursor = connection.cursor()
            cursor.execute("""SELECT id, medal_type, `count`, created_at
                FROM aurika_hryhorova.aurika_hw07_medal_counts ORDER BY id""")
            rows = cursor.fetchall()
            cursor.execute("SELECT UTC_TIMESTAMP(6), VERSION()")
            checked_at, version = cursor.fetchone()
            cursor.execute("SELECT medal, COUNT(*) FROM olympic_dataset.athlete_event_results GROUP BY medal")
            source = cursor.fetchall()
        finally:
            connection.close()
        body = """<!doctype html><html lang="uk"><meta charset="utf-8">
        <title>ДЗ7 — таблиця MySQL</title><style>
        body{font:20px Arial,sans-serif;margin:48px;color:#17232b;background:#fafbfc}
        h1{font-size:30px}h2{font-size:23px;margin-top:35px}
        table{border-collapse:collapse;background:white;margin:20px 0}
        td,th{border:1px solid #c5cdd3;padding:14px 22px;text-align:left}
        th{background:#e9eff3}code{font-size:17px}p{max-width:1100px;line-height:1.5}
        </style><h1>ДЗ7 — фактичні дані локальної MySQL</h1>
        <p>Live SELECT із власної навчальної бази. Ця сторінка є read-only переглядачем таблиці.</p>
        <h2>aurika_hryhorova.aurika_hw07_medal_counts</h2>
        <table><tr><th>id</th><th>medal_type</th><th>count</th><th>created_at UTC</th></tr>"""
        for row in rows:
            body += "<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in row) + "</tr>"
        body += "</table><h2>Контрольні кількості в olympic_dataset.athlete_event_results</h2>"
        body += "<table><tr><th>medal</th><th>COUNT(*)</th></tr>"
        for medal, count in source:
            body += f"<tr><td>{html.escape(medal or '(без медалі)')}</td><td>{count}</td></tr>"
        body += f"</table><p>Час SELECT UTC: {checked_at}<br>MySQL {version}; 127.0.0.1:13307</p>"
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 18088), Viewer).serve_forever()
