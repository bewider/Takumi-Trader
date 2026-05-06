"""User Manual / Explanation dialog for TAKUMI Trader."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import QDialog, QScrollArea, QLabel, QVBoxLayout, QWidget


_MANUAL_HTML = """
<style>
    body { font-family: 'Segoe UI', sans-serif; color: #222; }
    h1 { color: #2a5a8a; font-size: 18px; border-bottom: 2px solid #2a5a8a; padding-bottom: 4px; }
    h2 { color: #3a7abf; font-size: 15px; margin-top: 18px; border-bottom: 1px solid #ccc; padding-bottom: 3px; }
    h3 { color: #555; font-size: 13px; margin-top: 12px; }
    p, li { font-size: 12px; line-height: 1.5; }
    code { background: #eef; padding: 1px 4px; border-radius: 2px; font-size: 11px; }
    .key { background: #e8e8e8; border: 1px solid #bbb; padding: 1px 6px; border-radius: 3px;
           font-size: 11px; font-weight: bold; }
    table { border-collapse: collapse; margin: 6px 0; }
    td, th { border: 1px solid #ccc; padding: 4px 8px; font-size: 11px; }
    th { background: #e8eef5; }
    .green { color: #0a7a0a; font-weight: bold; }
    .red { color: #cc2222; font-weight: bold; }
</style>

<h1>TAKUMI Trader &mdash; User Manual</h1>

<h2>1. Overview</h2>
<p>TAKUMI Trader is a real-time currency strength scanner designed for <b>1-minute scalping</b>
on the Asia &amp; European sessions. It connects to <b>MetaTrader 5</b> (IC Markets),
calculates live currency strength across 4 timeframes, and fires alerts when
strong divergences between currency pairs are detected.</p>

<h2>2. The Main Table</h2>
<p>The top table shows <b>27 forex pairs</b> and the bottom table shows <b>8 individual currencies</b>.</p>

<h3>Columns</h3>
<table>
<tr><th>Column</th><th>Description</th></tr>
<tr><td><b>Pair / Currency</b></td><td>The forex pair (e.g. EURUSD) or individual currency (e.g. EUR)</td></tr>
<tr><td><b>Range</b></td><td>Session range consumed (%). Shows how much of the Asia+Europe average daily range
has been used today. Calculated from H1 candles filtered to UTC 0&ndash;13 hours, averaged over 2 weeks.
<br>Higher % = less room to move before session average is hit.</td></tr>
<tr><td><b>M1 / M5 / M15 / H1</b></td><td>Composite currency strength score for that timeframe, bounded &plusmn;10.
<br>The number shown is the <i>pair score</i> = base currency strength minus quote currency strength.</td></tr>
</table>

<h3>Color Coding</h3>
<table>
<tr><th>Score Range</th><th>Color</th><th>Meaning</th></tr>
<tr><td class="green">&ge; +6</td><td style="background:#004d00;color:#fff;">Dark Green</td><td>Extremely bullish</td></tr>
<tr><td class="green">+4 to +6</td><td style="background:#1a8a1a;color:#fff;">Green</td><td>Strong bullish</td></tr>
<tr><td>+2 to +4</td><td style="background:#66cc66;color:#000;">Light Green</td><td>Moderate bullish</td></tr>
<tr><td>0 to +2</td><td style="background:#ccffcc;color:#000;">Pale Green</td><td>Slight bullish</td></tr>
<tr><td>0 to -2</td><td style="background:#ffcccc;color:#000;">Pale Red</td><td>Slight bearish</td></tr>
<tr><td>-2 to -4</td><td style="background:#ff6666;color:#000;">Light Red</td><td>Moderate bearish</td></tr>
<tr><td>-4 to -6</td><td style="background:#cc0000;color:#fff;">Red</td><td>Strong bearish</td></tr>
<tr><td class="red">&le; -6</td><td style="background:#660000;color:#fff;">Dark Red</td><td>Extremely bearish</td></tr>
</table>

<h3>Arrows</h3>
<p>Small arrows next to scores show short-term momentum direction over the last few readings:</p>
<ul>
<li><b>&uarr;</b> Score is rising (getting more bullish)</li>
<li><b>&darr;</b> Score is falling (getting more bearish)</li>
<li><b>&bull;</b> Score is stable / flat</li>
</ul>

<h2>3. How Strength Is Calculated</h2>
<p>Each currency's strength is a composite of two (or three, on M1) indicators,
normalized using Z-score + tanh to stay within &plusmn;10:</p>

<h3>Indicators</h3>
<table>
<tr><th>Indicator</th><th>What It Measures</th><th>Formula</th></tr>
<tr><td><b>EMA-8 Displacement</b></td>
    <td>How far price is from its 8-period EMA, relative to volatility</td>
    <td><code>(close - EMA8) / ATR14</code></td></tr>
<tr><td><b>Weighted Micro-ROC</b></td>
    <td>Rate of change with exponential decay weighting, relative to volatility</td>
    <td><code>weighted_sum(ROC) / ATR14</code></td></tr>
<tr><td><b>Tick Velocity</b> (M1 only)</td>
    <td>Intra-candle momentum from candle open to current close</td>
    <td><code>(close - candle_open) / ATR14</code></td></tr>
</table>

<h3>Normalization</h3>
<p>Raw indicator values are converted to Z-scores using a rolling 200-bar buffer,
then compressed with: <code>score = 10 &times; tanh(z &times; sensitivity)</code><br>
This keeps all scores bounded between -10 and +10 regardless of market conditions.</p>

<h3>Timeframe Weights (M1)</h3>
<p>M1 uses 35% EMA Displacement + 35% Micro-ROC + 30% Tick Velocity.<br>
Other timeframes use 50% EMA Displacement + 50% Micro-ROC.</p>

<h2>4. Alerts</h2>
<p>Alerts fire when a <b>strong divergence</b> is detected between the base and quote
currency of a pair:</p>
<ul>
<li>Each currency's composite score must exceed the timeframe threshold
(M1: &plusmn;6.5, M5: &plusmn;6.0, M15: &plusmn;5.5, H1: &plusmn;5.0)</li>
<li>The two currencies must be moving in <i>opposite directions</i>
(one strong positive, one strong negative)</li>
<li>The total divergence spread must be &ge; 12.0 points</li>
<li>A per-pair cooldown (default 60 seconds) prevents repeated alerts</li>
</ul>

<p>Alert entries show: time, conviction score, pair, direction (BUY/SELL),
filter icons, ADR consumed %, and a clickable <b>[TRACK]</b> link.</p>

<h2>5. Conviction Scoring (Filter System)</h2>
<p>Every alert candidate is scored through up to 4 quality filters before firing.
The total conviction score (0&ndash;100) determines the alert tier:</p>

<table>
<tr><th>Tier</th><th>Conviction</th><th>Behavior</th></tr>
<tr><td class="green">FULL</td><td>&ge; threshold (default 70)</td><td>Shown with sound alert</td></tr>
<tr><td>DIMMED</td><td>45 &ndash; threshold</td><td>Shown without sound</td></tr>
<tr><td class="red">SUPPRESSED</td><td>&lt; 45</td><td>Hidden entirely</td></tr>
</table>

<h3>Filter Details</h3>
<table>
<tr><th>Filter</th><th>Button</th><th>Max Points</th><th>What It Checks</th></tr>
<tr><td>HTF Trend Regime</td><td><b>HTF</b></td><td>30</td>
    <td>Are the H4 and D1 timeframes aligned with the trade direction?
    <br>Bullish regime on higher TFs for a BUY = full points.
    Conflicting regime = reduced points.</td></tr>
<tr><td>Strength Velocity</td><td><b>VEL</b></td><td>20</td>
    <td>Is the currency strength moving fast in the trade direction?
    <br>Fast, aligned velocity = full points. Slow/opposing = reduced.</td></tr>
<tr><td>Isolation</td><td><b>Isolation</b></td><td>20</td>
    <td>Is the base currency clearly the strongest and the quote clearly the weakest
    (or vice versa)? Checks ranking position among all 8 currencies and the gap
    to the next-ranked currency.</td></tr>
<tr><td>ADR Position</td><td><b>ADR</b></td><td>15</td>
    <td>Is there enough daily range remaining for the trade to reach its target?
    <br>Low ADR consumed = full points. High ADR consumed (&gt;80%) = reduced.</td></tr>
</table>

<p><b>Important:</b> When a filter is toggled <b>OFF</b>, it contributes its full points
automatically (neutral). This means turning all filters OFF gives pre-filter behavior
&mdash; all alerts pass as FULL conviction.</p>

<p><b>Base points:</b> 15 points are always awarded regardless of filters, so the maximum
is 15 + 30 + 20 + 20 + 15 = 100.</p>

<h2>6. Filter Toolbar Buttons</h2>
<table>
<tr><th>Button</th><th>Appearance</th><th>Action</th></tr>
<tr><td><b>HTF</b></td><td>Toggle ON/OFF</td><td>Enable/disable the Higher Timeframe Trend Regime filter</td></tr>
<tr><td><b>VEL</b></td><td>Toggle ON/OFF</td><td>Enable/disable the Strength Velocity filter</td></tr>
<tr><td><b>Isolation</b></td><td>Toggle ON/OFF</td><td>Enable/disable the Isolation (ranking/gap) filter</td></tr>
<tr><td><b>ADR</b></td><td>Toggle ON/OFF</td><td>Enable/disable the ADR Position filter</td></tr>
<tr><td><b>Conv &ge; XX</b></td><td>Click to cycle</td><td>Cycles the conviction threshold: 50 &rarr; 60 &rarr; 70 &rarr; 80 &rarr; 50.
    <br>Only alerts scoring above this threshold get the FULL tier (with sound).</td></tr>
</table>

<p>Button states: <span style="background:#d4edda;color:#155724;padding:2px 6px;border-radius:3px;">
&#10004; ON (green)</span> &nbsp;
<span style="background:#f5e0e0;color:#993333;padding:2px 6px;border-radius:3px;">
&#10008; OFF (red)</span></p>

<h2>7. Trade Tracking</h2>
<p>Click <b>[TRACK]</b> on any alert to start tracking that trade. Up to <b>7 trades</b>
can be tracked simultaneously.</p>

<h3>Tracked Trade Display</h3>
<ul>
<li><b>Direction:</b> BUY or SELL with color coding</li>
<li><b>P/L Pips:</b> Live profit/loss in pips from entry price</li>
<li><b>Target:</b> Dynamic pip target calculated from ATR, session volatility, and conviction</li>
<li><b>Conviction:</b> The conviction score at the time the trade was opened</li>
<li><b>Time:</b> How many minutes the trade has been open</li>
<li><b>Exit Dots:</b> 5 colored dots representing exit detector votes
    <br>(<span class="green">green</span> = safe, <span style="color:orange;">orange</span> = caution,
    <span class="red">red</span> = exit signal)</li>
<li><b>Urgency:</b> SAFE / WATCH / CLOSE / URGENT based on vote count</li>
<li><b>[CLOSE TRADE]</b> Click to manually close the tracked trade</li>
</ul>

<h3>Exit Engine (5 Detectors)</h3>
<table>
<tr><th>#</th><th>Detector</th><th>What It Watches</th></tr>
<tr><td>1</td><td>Strength Reversal</td><td>Did the currency strength flip against the trade direction?</td></tr>
<tr><td>2</td><td>Momentum Stall</td><td>Has the M1 score stopped moving favorably?</td></tr>
<tr><td>3</td><td>Range Exhaustion</td><td>Has the pair consumed most of its daily range?</td></tr>
<tr><td>4</td><td>Time Decay</td><td>Has the trade been open too long for a scalp? (conviction-adjusted)</td></tr>
<tr><td>5</td><td>Adverse Flow</td><td>Is tick flow turning against the trade direction?</td></tr>
</table>
<p>Vote escalation: 0&ndash;1 votes = SAFE, 2 = WATCH, 3 = CLOSE, 4&ndash;5 = URGENT</p>

<h2>8. Range Column</h2>
<p>The <b>Range</b> column shows the percentage of the Asia+Europe session average daily range
that has been consumed today.</p>
<ul>
<li><b>Calculation:</b> Uses H1 candles filtered to UTC hours 0&ndash;13 (before NY open)
over the last 2 weeks (~10 trading days)</li>
<li><b>Why exclude US session?</b> Since you trade Asia &amp; Europe only, the relevant
range is what typically happens <i>before</i> the US session begins</li>
<li><b>Interpretation:</b> 30% = plenty of room left; 90% = pair has likely exhausted
its typical session move</li>
</ul>

<h2>9. Toolbar Buttons</h2>
<table>
<tr><th>Button</th><th>Function</th></tr>
<tr><td><b>Compact</b></td><td>Toggle compact mode: hides the filter toolbar, tracked trades panel,
and alert history to show only the data tables</td></tr>
<tr><td><b>Pin on Top</b></td><td>Keep the window always on top of other windows</td></tr>
<tr><td><b>Settings</b></td><td>Open settings: sound alerts, sound file, font size, cooldown timer</td></tr>
<tr><td><b>Explanation</b></td><td>Open this user manual</td></tr>
</table>

<h2>10. Settings</h2>
<table>
<tr><th>Setting</th><th>Description</th><th>Default</th></tr>
<tr><td>Enable sound alerts</td><td>Play a sound when a FULL conviction alert fires</td><td>ON</td></tr>
<tr><td>Sound file</td><td>Custom .wav or .mp3 file for alert sound</td><td>System default</td></tr>
<tr><td>Table font size</td><td>Font size for the pair/currency tables</td><td>10 pt</td></tr>
<tr><td>Alert cooldown</td><td>Minimum seconds between alerts for the same pair</td><td>60 seconds</td></tr>
</table>

<h2>11. Sessions</h2>
<p>The status bar shows the current trading session. Sessions are detected automatically
using timezone-aware logic with DST handling:</p>
<table>
<tr><th>Session</th><th>Approximate Hours (UTC)</th></tr>
<tr><td>Tokyo</td><td>00:00 &ndash; 08:00</td></tr>
<tr><td>Frankfurt</td><td>07:00 &ndash; 08:00</td></tr>
<tr><td>London</td><td>08:00 &ndash; 12:00</td></tr>
<tr><td>Overlap (London+NY)</td><td>12:00 &ndash; 16:00</td></tr>
<tr><td>New York PM</td><td>16:00 &ndash; 21:00</td></tr>
</table>

<h2>12. Data Update Frequency</h2>
<p>Timeframes are updated on a staggered schedule to minimize CPU and API load:</p>
<table>
<tr><th>Timeframe</th><th>Update Interval</th></tr>
<tr><td>M1</td><td>Every 1 second</td></tr>
<tr><td>M5</td><td>Every 3 seconds</td></tr>
<tr><td>M15</td><td>Every 10 seconds</td></tr>
<tr><td>H1</td><td>Every 30 seconds</td></tr>
<tr><td>H4 Regime</td><td>~Every 120 cycles (~2 min)</td></tr>
<tr><td>D1 Regime</td><td>~Every 300 cycles (~5 min)</td></tr>
</table>

<h2>13. Tips for Best Results</h2>
<ul>
<li><b>Wait for warmup:</b> The app needs ~200 bars of data to bootstrap Z-score normalization.
First few seconds may show zero scores.</li>
<li><b>Best during Asia/Europe:</b> The ADR and range calculations are tuned for
UTC 0&ndash;13 hours. US session data is excluded from range averages.</li>
<li><b>Use conviction filters:</b> Start with all filters ON and Conv &ge; 70.
Lower the threshold or disable filters only if you want more (but lower quality) alerts.</li>
<li><b>Watch the exit dots:</b> When 2+ dots turn orange/red, consider closing the trade
even if the target hasn't been hit.</li>
<li><b>Range column matters:</b> Avoid new trades when Range is above 80% &mdash;
the pair has likely used up its session move.</li>
</ul>

<p style="margin-top: 24px; color: #888; font-size: 10px; text-align: center;">
TAKUMI Trader &mdash; Built for precision scalping on IC Markets via MetaTrader 5</p>
"""


class ExplanationDialog(QDialog):
    """Scrollable user manual / explanation dialog."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("TAKUMI Trader \u2014 Explanation & User Manual")
        self.resize(700, 750)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        content = QLabel()
        content.setTextFormat(Qt.TextFormat.RichText)
        content.setWordWrap(True)
        content.setText(_MANUAL_HTML)
        content.setFont(QFont("Segoe UI", 10))
        content.setContentsMargins(20, 16, 20, 16)

        scroll.setWidget(content)
        layout.addWidget(scroll)

        self.setStyleSheet(
            """
            QDialog { background: #ffffff; }
            QScrollArea { border: none; background: #ffffff; }
            QLabel { background: #ffffff; color: #222222; }
            """
        )
