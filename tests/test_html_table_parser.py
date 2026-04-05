from bs4 import BeautifulSoup

from canadastats.sources.base import parse_html_tables


def test_parse_html_tables_standings() -> None:
    html = """
    <h3>Regional Standings</h3>
    <table>
      <thead><tr><th>#</th><th>Team</th><th>GP</th><th>W</th><th>L</th><th>PTS</th></tr></thead>
      <tbody>
        <tr><td>1</td><td>Woodlawn</td><td>23</td><td>21</td><td>2</td><td>62</td></tr>
      </tbody>
    </table>
    """
    soup = BeautifulSoup(html, "lxml")
    tables = parse_html_tables(soup)
    assert len(tables) == 1
    assert tables[0].title == "Regional Standings"
    assert tables[0].rows[0]["Team"] == "Woodlawn"
    assert tables[0].rows[0]["PTS"] == "62"
