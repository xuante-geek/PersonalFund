#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import requests

CSV_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030", "gbk")
DECIMAL_PLACES = 6


@dataclass
class HoldingRow:
    line_no: int
    target_name: str
    target_code: str
    target_amount_raw: str
    target_cost_raw: str
    quote_url: str
    product_variable: str
    assets_variable: str
    industry_variable: str


def read_csv_rows(csv_path: Path) -> Tuple[List[List[str]], str]:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            with csv_path.open("r", encoding=encoding, newline="") as f:
                return list(csv.reader(f)), encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    raise RuntimeError(f"Cannot decode CSV: {csv_path}") from last_error


def write_csv_rows(csv_path: Path, rows: List[List[object]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def normalize_code(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if re.fullmatch(r"-?\d+(?:\.0+)?", text):
        try:
            return str(int(float(text)))
        except ValueError:
            return text
    return text


def looks_like_target_code(value: str) -> bool:
    text = value.strip().lower()
    if not text:
        return False
    if re.fullmatch(r"\d{6}", text):
        return True
    if re.fullmatch(r"(sh|sz|jj)\d{6}", text):
        return True
    return False


def extract_target_digits(value: str) -> str:
    text = value.strip().lower()
    match = re.fullmatch(r"(?:sh|sz|jj)?(\d{6})", text)
    if not match:
        return ""
    return match.group(1)


def normalize_quote_url(target_code: str, quote_url: str, product_variable: str) -> str:
    product = normalize_code(product_variable)
    if product == "3":
        digits = extract_target_digits(target_code)
        if digits:
            return f"https://gu.qq.com/jj{digits}"
    return quote_url.strip()


def parse_number(value: str) -> float:
    cleaned = value.replace(",", "").replace("，", "").strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)
    if not match:
        raise ValueError(f"Cannot parse number from '{value}'")
    return float(match.group(0))


def round6(value: float) -> float:
    return round(float(value), DECIMAL_PLACES)


def extract_decimal_candidates(text: str, pattern: str) -> List[float]:
    candidates: List[float] = []
    for match in re.finditer(pattern, text, flags=re.IGNORECASE):
        value_text = ""
        if match.lastindex:
            value_text = match.group(1)
        else:
            value_text = match.group(0)
        value = parse_number(value_text)
        candidates.append(value)
    return candidates


def extract_nav_4dp_near_netvalue(text: str) -> float | None:
    content = str(text or "")
    if not content:
        return None

    keyword_matches = list(re.finditer(r"(单位)?净值", content, flags=re.IGNORECASE))
    number_matches = list(re.finditer(r"\d+\.\d{4}", content))
    if not keyword_matches or not number_matches:
        return None

    best_value: float | None = None
    best_distance: int | None = None

    for kw in keyword_matches:
        kw_pos = kw.start()
        for num in number_matches:
            num_pos = num.start()
            distance = abs(num_pos - kw_pos)
            # Keep numbers that are reasonably close to "净值" context.
            if distance > 120:
                continue
            try:
                value = float(num.group(0))
            except ValueError:
                continue
            if value <= 0:
                continue
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_value = value

    return best_value


def choose_candidate_for_product(
    candidates: List[float], product_variable: str
) -> float | None:
    if not candidates:
        return None

    product = normalize_code(product_variable)
    filtered = [value for value in candidates if 0.0001 <= value <= 100000]
    if not filtered:
        return None

    # 基金净值通常在更小数值区间，优先取更“像净值”的值。
    if product == "3":
        nav_like = [value for value in filtered if 0.1 <= value <= 20]
        if nav_like:
            return nav_like[0]
        return filtered[0]

    # 股票/ETF：优先取第一组匹配到的“价格型”候选。
    return filtered[0]


def extract_price_from_html_text(
    raw_html: str,
    body_text: str,
    product_variable: str,
) -> float | None:
    product = normalize_code(product_variable)

    # 优先使用关键词附近的价格，降低误匹配页面其它数值（如日期、涨跌幅、成交量）。
    fund_patterns = [
        r'"dwjz"\s*:\s*"(\d+\.\d{2,6})"',
        r'"gsz"\s*:\s*"(\d+\.\d{2,6})"',
        r'"nav"\s*:\s*"(\d+\.\d{2,6})"',
        r"净值[^0-9]{0,16}(\d+\.\d{2,6})",
    ]
    stock_patterns = [
        r'"curPrice"\s*:\s*"(\d+\.\d{2,6})"',
        r'"latestPrice"\s*:\s*"(\d+\.\d{2,6})"',
        r'"price"\s*:\s*"(\d+\.\d{2,6})"',
        r"最新价[^0-9]{0,16}(\d+\.\d{2,6})",
        r"现价[^0-9]{0,16}(\d+\.\d{2,6})",
        r"(\d+\.\d{2,6})\s*元",
    ]
    generic_patterns = [
        r'"price"\s*:\s*(\d+\.\d{2,6})',
        r'"close"\s*:\s*(\d+\.\d{2,6})',
        r"交易[^0-9]{0,12}(\d+\.\d{2,6})",
        r"行情[^0-9]{0,12}(\d+\.\d{2,6})",
    ]

    text_sources = [raw_html, body_text]
    preferred_patterns = fund_patterns if product == "3" else stock_patterns

    if product == "3":
        # Extra fallback: pick 4-decimal price close to "净值" keywords.
        for source in (body_text, raw_html):
            value = extract_nav_4dp_near_netvalue(source)
            if value is not None:
                return value

    for source in text_sources:
        if not source:
            continue
        for pattern in preferred_patterns:
            candidates = extract_decimal_candidates(source, pattern)
            picked = choose_candidate_for_product(candidates, product)
            if picked is not None:
                return picked

    for source in text_sources:
        if not source:
            continue
        for pattern in generic_patterns:
            candidates = extract_decimal_candidates(source, pattern)
            picked = choose_candidate_for_product(candidates, product)
            if picked is not None:
                return picked

    return None


def load_variable_mapping(csv_path: Path) -> Dict[str, str]:
    rows, _ = read_csv_rows(csv_path)
    mapping: Dict[str, str] = {}
    for row in rows[1:]:
        if len(row) < 2:
            continue
        label = row[0].strip()
        code = normalize_code(row[1])
        if code:
            mapping[code] = label
    return mapping


def load_holdings(csv_path: Path) -> Tuple[List[HoldingRow], str]:
    rows, encoding = read_csv_rows(csv_path)
    holdings: List[HoldingRow] = []
    for index, raw_row in enumerate(rows[1:], start=2):
        row = (raw_row + [""] * 8)[:8]
        row = [cell.strip() for cell in row]
        if not any(row):
            continue
        # Fixed format:
        # A-H: name,code,amount,cost,url,product,assets,industry
        target_code = row[1]
        if not looks_like_target_code(target_code):
            raise ValueError(f"line {index}: invalid target_code '{row[1]}'")

        normalized_quote_url = normalize_quote_url(
            target_code=target_code,
            quote_url=row[4],
            product_variable=row[5],
        )

        holdings.append(
            HoldingRow(
                line_no=index,
                target_name=row[0],
                target_code=target_code,
                target_amount_raw=row[2],
                target_cost_raw=row[3],
                quote_url=normalized_quote_url,
                product_variable=row[5],
                assets_variable=row[6],
                industry_variable=row[7],
            )
        )
    return holdings, encoding


class PriceFetcher:
    def __init__(self, timeout: float) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )
        self.timeout = timeout

    def _extract_tencent_symbol_from_url(self, quote_url: str) -> str:
        text = quote_url.strip().lower()
        if not text:
            return ""
        # Examples:
        # https://gu.qq.com/sh603259/gp
        # https://gu.qq.com/sz159740
        # https://gu.qq.com/jj001410
        match = re.search(r"gu\.qq\.com/((?:sh|sz|jj)\d{6})", text)
        if match:
            return match.group(1)
        return ""

    def _build_tencent_symbols(
        self,
        target_code: str,
        quote_url: str,
        product_variable: str,
    ) -> List[str]:
        symbols: List[str] = []

        code_text = target_code.strip().lower()
        product = normalize_code(product_variable)

        digits = ""
        match_with_prefix = re.fullmatch(r"(sh|sz|jj)(\d{6})", code_text)
        if match_with_prefix:
            digits = match_with_prefix.group(2)
        elif re.fullmatch(r"\d{6}", code_text):
            digits = code_text

        # For active funds(product=3), prioritize NAV-style symbol `jj`.
        if product == "3" and digits:
            symbols.append(f"jj{digits}")

        from_url = self._extract_tencent_symbol_from_url(quote_url)
        if from_url:
            symbols.append(from_url)

        if match_with_prefix:
            symbols.append(code_text)
            if product == "3":
                symbols.extend([f"sz{digits}", f"sh{digits}"])
        elif digits:
            if product == "3":
                symbols.extend([f"sz{digits}", f"sh{digits}"])
            elif digits.startswith(("5", "6", "9")):
                symbols.extend([f"sh{digits}", f"sz{digits}"])
            else:
                symbols.extend([f"sz{digits}", f"sh{digits}"])

        # Keep order while removing duplicates.
        deduped: List[str] = []
        seen = set()
        for symbol in symbols:
            if symbol and symbol not in seen:
                deduped.append(symbol)
                seen.add(symbol)
        return deduped

    def _pick_tencent_price_from_parts(
        self,
        parts: List[str],
        product_variable: str,
    ) -> float | None:
        product = normalize_code(product_variable)

        for index in (3, 4, 5):
            if index >= len(parts):
                continue
            try:
                candidate = float(parts[index])
            except (TypeError, ValueError):
                continue
            if candidate <= 0:
                continue
            if product == "3" and candidate > 1000:
                continue
            return candidate

        for raw in parts[1:20]:
            try:
                candidate = float(raw)
            except (TypeError, ValueError):
                continue
            if not (0.0001 <= candidate <= 100000):
                continue
            if product == "3" and candidate > 1000:
                continue
            return candidate
        return None

    def _fetch_tencent_quote_by_symbol(
        self,
        symbol: str,
        product_variable: str,
    ) -> Tuple[float | None, str]:
        if not symbol:
            return None, "empty-symbol"
        try:
            product = normalize_code(product_variable)
            if product == "3" and not symbol.startswith("jj"):
                return None, "skip-non-jj-for-fund-nav"

            api_url = f"https://qt.gtimg.cn/q={symbol}"
            resp = self.session.get(api_url, timeout=self.timeout)
            resp.raise_for_status()
            text = resp.text.strip()
            payload_match = re.search(r'="([^"]+)"', text)
            if not payload_match:
                return None, "no-payload"
            parts = payload_match.group(1).split("~")
            picked = self._pick_tencent_price_from_parts(parts, product_variable)
            if picked is None:
                return None, "no-valid-price"
            return picked, ""
        except Exception as exc:
            return None, str(exc)

    def _fetch_tencent_quote_from_url_or_code(
        self,
        quote_url: str,
        target_code: str,
        product_variable: str,
    ) -> Tuple[float | None, str, str]:
        symbols = self._build_tencent_symbols(target_code, quote_url, product_variable)
        errors: List[str] = []
        for symbol in symbols:
            price, error = self._fetch_tencent_quote_by_symbol(symbol, product_variable)
            if price is not None:
                return price, symbol, ""
            if error:
                errors.append(f"{symbol}:{error}")
        return None, "", "; ".join(errors)

    def fetch_price(
        self,
        quote_url: str,
        target_code: str = "",
        product_variable: str = "",
    ) -> Tuple[float, str]:
        # Tencent URL mode: prefer direct quote endpoint; avoid relying on page HTML rendering.
        tencent_price, tencent_symbol, tencent_debug = self._fetch_tencent_quote_from_url_or_code(
            quote_url,
            target_code,
            product_variable,
        )
        if tencent_price is not None:
            return tencent_price, f"{tencent_price} (tencent-api:{tencent_symbol})"

        tencent_note = f"; tencent_api={tencent_debug}" if tencent_debug else ""
        if not quote_url:
            raise ValueError(f"quote_url is empty and tencent symbol lookup failed{tencent_note}")

        # URL fallback: parse visible HTML/script text without DOM locator dependency.
        try:
            response = self.session.get(quote_url, timeout=self.timeout)
            response.raise_for_status()
            raw_html = response.text
        except Exception as exc:
            raise ValueError(f"{exc}{tencent_note}") from exc
        body_text = " ".join(re.findall(r"[\u4e00-\u9fffA-Za-z0-9\.\-_%]+", raw_html))
        picked = extract_price_from_html_text(raw_html, body_text, product_variable)
        if picked is not None:
            return picked, f"{picked} (url-content-fallback)"

        raise ValueError(f"No price candidate matched by Tencent API or URL content patterns{tencent_note}")


def generate_daily_data(
    holdings_csv: Path,
    product_ref_csv: Path,
    assets_ref_csv: Path,
    industry_ref_csv: Path,
    output_csv: Path,
    archive_dir: Path | None,
    timeout: float,
) -> int:
    holdings, holdings_encoding = load_holdings(holdings_csv)
    product_map = load_variable_mapping(product_ref_csv)
    assets_map = load_variable_mapping(assets_ref_csv)
    industry_map = load_variable_mapping(industry_ref_csv)

    fetcher = PriceFetcher(timeout=timeout)
    today = dt.date.today().isoformat()

    header = [
        "date",
        "target_name",
        "target_code",
        "target_amount",
        "target_cost",
        "target_price",
        "target_value",
        "target_pnl",
        "target_return_rate",
        "quote_url",
        "product_variable",
        "product_name",
        "assets_variable",
        "assets_name",
        "industry_variable",
        "industry_name",
        "fetch_status",
        "fetch_note",
    ]
    output_rows: List[List[object]] = [header]

    success_count = 0
    error_count = 0
    total_value = 0.0
    total_cost = 0.0

    for holding in holdings:
        product_code = normalize_code(holding.product_variable)
        assets_code = normalize_code(holding.assets_variable)
        industry_code = normalize_code(holding.industry_variable)
        product_name = product_map.get(product_code, "")
        assets_name = assets_map.get(assets_code, "")
        industry_name = industry_map.get(industry_code, "")

        target_price_text = ""
        target_price_val: float | None = None
        target_value: float | None = None
        target_pnl: float | None = None
        target_return_rate: float | None = None

        fetch_status = "ok"
        fetch_note = ""

        try:
            target_amount = round6(float(holding.target_amount_raw))
            target_cost = round6(float(holding.target_cost_raw))
            target_price_val, target_price_text = fetcher.fetch_price(
                holding.quote_url,
                holding.target_code,
                holding.product_variable,
            )
            target_price_val = round6(target_price_val)
            target_value = round6(target_amount * target_price_val)
            target_pnl = round6(target_value - target_cost)
            target_return_rate = (
                round6((target_value / target_cost - 1.0) * 100.0) if target_cost else None
            )
            total_value += target_value
            total_cost += target_cost
            success_count += 1
        except Exception as exc:
            fetch_status = "error"
            fetch_note = f"line {holding.line_no}: {exc}"
            error_count += 1
            target_amount = holding.target_amount_raw
            target_cost = holding.target_cost_raw

        output_rows.append(
            [
                today,
                holding.target_name,
                holding.target_code,
                target_amount,
                target_cost,
                target_price_val if target_price_val is not None else "",
                target_value if target_value is not None else "",
                target_pnl if target_pnl is not None else "",
                target_return_rate if target_return_rate is not None else "",
                holding.quote_url,
                product_code,
                product_name,
                assets_code,
                assets_name,
                industry_code,
                industry_name,
                fetch_status,
                target_price_text if fetch_status == "ok" else fetch_note,
            ]
        )

    total_cost_rounded = round6(total_cost) if success_count else ""
    total_value_rounded = round6(total_value) if success_count else ""
    total_pnl_rounded = round6(total_value - total_cost) if success_count else ""
    total_return_rate_rounded = (
        round6((total_value / total_cost - 1.0) * 100.0) if success_count and total_cost else ""
    )

    output_rows.append(
        [
            today,
            "__TOTAL__",
            "",
            "",
            total_cost_rounded,
            "",
            total_value_rounded,
            total_pnl_rounded,
            total_return_rate_rounded,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "summary",
            f"holdings_encoding={holdings_encoding}; success={success_count}; error={error_count}",
        ]
    )

    write_csv_rows(output_csv, output_rows)

    if archive_dir is not None:
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"daily_data_{today}.csv"
        shutil.copyfile(output_csv, archive_path)
        print(f"Archived to: {archive_path}")

    print(f"Wrote: {output_csv}")
    print(f"Success: {success_count}, Error: {error_count}")
    return 0 if error_count == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate daily_data.csv from holdings (Python fetcher or Node Playwright fetcher)."
    )
    parser.add_argument(
        "--holdings",
        type=Path,
        default=Path("data/input/current_holdings.csv"),
        help="Path to current_holdings.csv",
    )
    parser.add_argument(
        "--product-ref",
        type=Path,
        default=Path("data/reference/product_variable.csv"),
        help="Path to product_variable.csv",
    )
    parser.add_argument(
        "--assets-ref",
        type=Path,
        default=Path("data/reference/assets_variable.csv"),
        help="Path to assets_variable.csv",
    )
    parser.add_argument(
        "--industry-ref",
        type=Path,
        default=Path("data/reference/industry_variable.csv"),
        help="Path to industry_variable.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/output/daily_data.csv"),
        help="Output daily_data.csv path",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("data/archive/daily_data"),
        help="Archive directory; pass empty string to disable",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="HTTP request timeout in seconds",
    )
    parser.add_argument(
        "--engine",
        choices=("auto", "playwright-node", "python"),
        default="python",
        help="Price fetch engine. default=python (URL content extraction without Playwright).",
    )
    parser.add_argument(
        "--node-bin",
        default="node",
        help="Node.js executable path/name for playwright-node engine.",
    )
    parser.add_argument(
        "--playwright-script",
        type=Path,
        default=Path("scripts/generate_daily_data_playwright.mjs"),
        help="Node Playwright script path.",
    )
    return parser


def run_playwright_node(node_bin: str, playwright_script: Path) -> int:
    project_root = Path(__file__).resolve().parents[1]
    script_path = playwright_script if playwright_script.is_absolute() else project_root / playwright_script
    node_path = shutil.which(node_bin)
    if not node_path:
        raise RuntimeError(f"Node.js executable not found: {node_bin}")
    if not script_path.exists():
        raise RuntimeError(f"Playwright script not found: {script_path}")

    command = [node_path, str(script_path)]
    print(f"Running Node Playwright engine: {' '.join(command)}")
    result = subprocess.run(command, cwd=str(project_root))
    return result.returncode


def run_python_engine(args: argparse.Namespace, archive_dir: Path | None) -> int:
    return generate_daily_data(
        holdings_csv=args.holdings,
        product_ref_csv=args.product_ref,
        assets_ref_csv=args.assets_ref,
        industry_ref_csv=args.industry_ref,
        output_csv=args.output,
        archive_dir=archive_dir,
        timeout=args.timeout,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    archive_dir: Path | None
    if str(args.archive_dir).strip() == "":
        archive_dir = None
    else:
        archive_dir = args.archive_dir

    if args.engine == "python":
        return run_python_engine(args, archive_dir)

    if args.engine == "playwright-node":
        return run_playwright_node(args.node_bin, args.playwright_script)

    # auto mode: prefer Node Playwright; fallback to Python when Node is unavailable or execution fails.
    try:
        node_rc = run_playwright_node(args.node_bin, args.playwright_script)
        if node_rc == 0:
            return 0
        print(f"Playwright engine failed with exit code {node_rc}, fallback to Python engine.")
        return run_python_engine(args, archive_dir)
    except RuntimeError as exc:
        print(f"Playwright engine unavailable, fallback to Python engine: {exc}")
        return run_python_engine(args, archive_dir)


if __name__ == "__main__":
    sys.exit(main())
