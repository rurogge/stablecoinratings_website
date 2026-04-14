/**
 * StablecoinRatings — Frontend App
 * Loads JSON data, renders table/charts, handles theme + modal.
 * All data fetched at build-time; JS only renders pre-generated JSON.
 */

"use strict";

// ── Global state ────────────────────────────────────────────────────────────
let allData       = null;   // full JSON from ratings_full.json
let currentTable  = "usd"; // "usd" | "nonusd"
let charts        = {};     // Chart.js instances

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    initTheme();

    // Fetch data
    try {
        const res = await fetch("data/ratings_full.json", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        allData = await res.json();
        document.getElementById("skeletonLoader").style.display = "none";
    } catch (err) {
        console.error("Failed to load data:", err);
        document.getElementById("skeletonLoader").innerHTML =
            `<div style="padding:2rem;text-align:center;color:var(--text-muted);font-size:14px;">
                Could not load ratings data. Please try again later.
                <br><br><code style="font-size:12px;">${err.message}</code>
            </div>`;
        return;
    }

    renderStatsBar();
    renderCharts();
    renderTable(currentTable);
    initTableTabs();
    initModal();

    // Display data freshness
    const updated = allData.run_id
        ? `Data refreshed: ${new Date(allData.run_id).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" })} UTC`
        : "";
    document.getElementById("dataNote").textContent = updated;
});

// ── Theme toggle ─────────────────────────────────────────────────────────────
function initTheme() {
    const stored = localStorage.getItem("scr_theme");
    const preferred = stored || (window.matchMedia("(prefers-color: dark)").matches ? "dark" : "light");
    applyTheme(preferred);

    document.getElementById("themeToggle").addEventListener("click", () => {
        const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
        applyTheme(next);
        localStorage.setItem("scr_theme", next);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    const sun  = document.querySelector(".icon-sun");
    const moon = document.querySelector(".icon-moon");
    if (theme === "dark") {
        sun.style.display  = "";
        moon.style.display = "none";
    } else {
        sun.style.display  = "none";
        moon.style.display = "";
    }
    // Re-render charts with new theme colors
    if (allData) renderCharts();
}

// ── Helpers ──────────────────────────────────────────────────────────────────
const GRADE_CLASS = {
    "A+":"a","A":"a","A-":"a",
    "B+":"b","B":"b","B-":"b",
    "C+":"c","C":"c","C-":"c",
    "D+":"d","D":"d","D-":"d",
};

function gradeClass(letter) {
    return "grade-" + (GRADE_CLASS[letter] || "c");
}

function gradeColor(letter, type = "bg") {
    const map = {
        a: { bg: "rgba(34,197,94,0.15)",  text: "#22c55e" },
        b: { bg: "rgba(59,130,246,0.15)", text: "#3b82f6" },
        c: { bg: "rgba(245,158,11,0.15)", text: "#f59e0b" },
        d: { bg: "rgba(239,68,68,0.15)",  text: "#ef4444" },
    };
    return map[GRADE_CLASS[letter] || "c"][type];
}

function pillarColor(score) {
    if (score >= 80) return "#22c55e";
    if (score >= 60) return "#3b82f6";
    if (score >= 40) return "#f59e0b";
    return "#ef4444";
}

function formatMcap(mcap) {
    if (!mcap && mcap !== 0) return "—";
    if (mcap >= 1e12) return `$${(mcap/1e12).toFixed(2)}T`;
    if (mcap >= 1e9)  return `$${(mcap/1e9).toFixed(2)}B`;
    if (mcap >= 1e6)  return `$${(mcap/1e6).toFixed(1)}M`;
    return `$${mcap.toLocaleString()}`;
}

function padScore(n) { return n.toFixed(1); }

// ── Stats bar ────────────────────────────────────────────────────────────────
function renderStatsBar() {
    const usd = allData.usd_stablecoins || [];
    document.getElementById("statCoins").textContent = usd.length;

    const aCount = usd.filter(r => r.letter.startsWith("A")).length;
    const bCount = usd.filter(r => r.letter.startsWith("B")).length;
    const cdCount = usd.filter(r => r.letter.startsWith("C") || r.letter.startsWith("D")).length;

    document.getElementById("statA").textContent = aCount;
    document.getElementById("statB").textContent = bCount;
    document.getElementById("statC").textContent = cdCount;
}

// ── Charts ───────────────────────────────────────────────────────────────────
const CHART_FONT = { family: "'Inter', sans-serif", size: 12 };

function chartDefaults() {
    return {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: getComputedStyle(document.documentElement).getPropertyValue("--bg-card").trim() || "#1a1d25",
                titleColor:       getComputedStyle(document.documentElement).getPropertyValue("--text-primary").trim()  || "#e8eaed",
                bodyColor:         getComputedStyle(document.documentElement).getPropertyValue("--text-secondary").trim() || "#9aa0a8",
                borderColor:       getComputedStyle(document.documentElement).getPropertyValue("--border").trim()       || "#252a35",
                borderWidth: 1,
                padding: 10,
                cornerRadius: 6,
            },
        },
        scales: {
            x: {
                ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text").trim() || "#9aa0a8", font: CHART_FONT },
                grid:  { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-grid").trim() || "#252a35" },
            },
            y: {
                ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text").trim() || "#9aa0a8", font: CHART_FONT },
                grid:  { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-grid").trim() || "#252a35" },
            },
        },
    };
}

function renderCharts() {
    const usd = allData?.usd_stablecoins || [];
    if (!usd.length) return;

    const gradeCounts = {};
    usd.forEach(r => { gradeCounts[r.letter] = (gradeCounts[r.letter] || 0) + 1; });

    const gradeOrder = ["A+","A","A-","B+","B","B-","C+","C","C-","D+","D","D-"];
    const labels = gradeOrder.filter(g => gradeCounts[g]);
    const values = labels.map(g => gradeCounts[g]);
    const colors = labels.map(g => {
        const c = GRADE_CLASS[g] || "c";
        return { a:"#22c55e", b:"#3b82f6", c:"#f59e0b", d:"#ef4444" }[c];
    });

    // Destroy existing
    if (charts.dist) charts.dist.destroy();
    if (charts.mcap) charts.mcap.destroy();

    // Distribution bar chart
    const ctxDist = document.getElementById("distChart");
    if (ctxDist) {
        charts.dist = new Chart(ctxDist, {
            type: "bar",
            data: {
                labels,
                datasets: [{ data: values, backgroundColor: colors.map(c => c + "99"), borderColor: colors, borderWidth: 1.5, borderRadius: 4 }],
            },
            options: {
                ...chartDefaults(),
                indexAxis: "x",
                plugins: { ...chartDefaults().plugins,
                    tooltip: { ...chartDefaults().plugins.tooltip,
                        callbacks: { label: ctx => `${ctx.parsed.y} coin${ctx.parsed.y !== 1 ? "s" : ""}` }
                    }
                },
                scales: {
                    x: { ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text") || "#9aa0a8", font: CHART_FONT }, grid: { display: false } },
                    y: { ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text") || "#9aa0a8", font: CHART_FONT, stepSize: 1 }, grid: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-grid") || "#252a35" }, beginAtZero: true },
                },
            },
        });
    }

    // Market cap horizontal bar chart (top 10)
    const top10 = [...usd].sort((a,b) => (b.mcap||0) - (a.mcap||0)).slice(0, 10);
    const mcapLabels = top10.map(r => r.symbol);
    const mcapValues = top10.map(r => ((r.mcap||0) / 1e9)); // in billions

    const ctxMcap = document.getElementById("mcapChart");
    if (ctxMcap) {
        charts.mcap = new Chart(ctxMcap, {
            type: "bar",
            data: {
                labels: mcapLabels,
                datasets: [{
                    data: mcapValues,
                    backgroundColor: mcapLabels.map(sym => {
                        const coin = usd.find(r => r.symbol === sym);
                        return gradeColor(coin?.letter || "C", "text") + "40";
                    }),
                    borderColor: mcapLabels.map(sym => {
                        const coin = usd.find(r => r.symbol === sym);
                        return gradeColor(coin?.letter || "C", "text");
                    }),
                    borderWidth: 1.5,
                    borderRadius: 4,
                }],
            },
            options: {
                ...chartDefaults(),
                indexAxis: "y",
                plugins: {
                    ...chartDefaults().plugins,
                    tooltip: {
                        ...chartDefaults().plugins.tooltip,
                        callbacks: { label: ctx => `$${ctx.parsed.x.toFixed(2)}B` }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text") || "#9aa0a8", font: CHART_FONT, callback: v => `$${v}B` },
                        grid: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-grid") || "#252a35" },
                    },
                    y: {
                        ticks: { color: getComputedStyle(document.documentElement).getPropertyValue("--chart-text") || "#9aa0a8", font: { ...CHART_FONT, weight: "600" } },
                        grid: { display: false },
                    },
                },
            },
        });
    }
}

// ── Table rendering ──────────────────────────────────────────────────────────
function pillarBarHTML(score) {
    const color = pillarColor(score);
    return `<div class="pillar-bar">
        <div class="pillar-bar__track">
            <div class="pillar-bar__fill" style="width:${score}%;background:${color};"></div>
        </div>
        <div style="font-size:10px;font-family:var(--font-mono);font-weight:600;color:${color};">${score.toFixed(0)}</div>
    </div>`;
}

function renderTable(table) {
    currentTable = table;

    if (table === "usd") {
        document.getElementById("usdTable").style.display = "";
        document.getElementById("nonusdTable").style.display = "none";
        renderUSDRows();
    } else {
        document.getElementById("usdTable").style.display = "none";
        document.getElementById("nonusdTable").style.display = "";
        renderNonUSDRows();
    }
}

function renderUSDRows() {
    const usd = allData?.usd_stablecoins || [];
    const tbody = document.getElementById("tableBodyUSD");
    tbody.innerHTML = "";

    usd.forEach((coin, i) => {
        const rank    = coin.rank ? `#${coin.rank}` : "—";
        const gc      = gradeClass(coin.letter);
        const mcapStr = formatMcap(coin.mcap);
        const flags   = (coin.flags || []).map(f =>
            `<span class="flag">${f.replace(/_/g, " ")}</span>`
        ).join("");

        const row = document.createElement("tr");
        row.style.cursor = "pointer";
        row.addEventListener("click", () => openModal(coin));

        row.innerHTML = `
            <td class="td-rank">${i+1}</td>
            <td>
                <div class="td-coin">
                    <div>
                        <div class="td-coin__symbol">${coin.symbol}</div>
                        <div class="td-coin__name">${coin.name}</div>
                    </div>
                </div>
            </td>
            <td><span class="grade-badge ${gc}">${coin.letter}</span> ${flags}</td>
            <td class="td-score">${coin.total.toFixed(1)}</td>
            <td>${pillarBarHTML(coin.peg)}</td>
            <td>${pillarBarHTML(coin.reserve)}</td>
            <td>${pillarBarHTML(coin.liquidity)}</td>
            <td>${pillarBarHTML(coin.management)}</td>
            <td>${pillarBarHTML(coin.smart_contract)}</td>
            <td>${pillarBarHTML(coin.decentralization)}</td>
            <td class="td-mcap">${mcapStr}</td>
        `;
        tbody.appendChild(row);
    });
}

function renderNonUSDRows() {
    const nonUSD = allData?.non_usd_stablecoins || [];
    const tbody = document.getElementById("tableBodyNonUSD");
    tbody.innerHTML = "";

    const typeLabel = { gold:"Gold-backed", sgd_fiat:"SGD-backed", reflex:"Reflex Index", eur_fiat:"EUR-backed" };

    nonUSD.forEach((coin, i) => {
        const gc      = gradeClass(coin.letter);
        const mcapStr = formatMcap(coin.mcap);
        const typeStr = typeLabel[coin.type] || coin.type || "—";

        const row = document.createElement("tr");
        row.style.cursor = "pointer";
        row.addEventListener("click", () => openModal(coin));
        row.innerHTML = `
            <td class="td-rank">${i+1}</td>
            <td>
                <div class="td-coin">
                    <div>
                        <div class="td-coin__symbol">${coin.symbol}</div>
                        <div class="td-coin__name">${coin.name}</div>
                    </div>
                </div>
            </td>
            <td style="font-size:13px;color:var(--text-muted);">${typeStr}</td>
            <td><span class="grade-badge ${gc}">${coin.letter}</span></td>
            <td class="td-score">${coin.total.toFixed(1)}</td>
            <td class="td-mcap">${mcapStr}</td>
        `;
        tbody.appendChild(row);
    });
}

function initTableTabs() {
    document.querySelectorAll(".tab").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            renderTable(btn.dataset.table);
        });
    });
}

// ── Modal ───────────────────────────────────────────────────────────────────
function openModal(coin) {
    document.getElementById("modalTitle").textContent   = `${coin.symbol} — ${coin.name}`;
    document.getElementById("modalSubtitle").textContent = `${coin.type?.replace(/_/g, " ") || "USD stablecoin"} · ${coin.backing || "—"}-backed`;
    document.getElementById("modalGrade").textContent   = coin.letter;
    document.getElementById("modalGrade").className     = `grade-badge grade-badge--large ${gradeClass(coin.letter)}`;
    document.getElementById("modalScore").textContent  = `${coin.total.toFixed(1)} / 100`;
    document.getElementById("modalRank").textContent    = coin.rank ? `Rank #${coin.rank}` : "Unranked";

    // Pillar scores
    const pillars = [
        { key:"peg",           label:"Peg Stability" },
        { key:"reserve",       label:"Reserve Quality" },
        { key:"liquidity",     label:"Liquidity" },
        { key:"management",    label:"Management" },
        { key:"smart_contract",label:"Smart Contract" },
        { key:"decentralization", label:"Decentralization" },
    ];
    const pg = document.getElementById("modalPillars");
    pg.innerHTML = pillars.map(p => {
        const score = coin[p.key];
        const color = pillarColor(score);
        return `<div class="pillar-item">
            <div class="pillar-item__name">${p.label}</div>
            <div class="pillar-item__score" style="color:${color};">${score.toFixed(1)}</div>
            <div class="pillar-item__bar"><div class="pillar-item__bar-fill" style="width:${score}%;background:${color};"></div></div>
        </div>`;
    }).join("");

    // Key metrics
    const sub = coin.sub_scores || {};
    const metrics = [
        { label:"Market Cap",     value: formatMcap(coin.mcap) },
        { label:"24h Volume",     value: formatMcap(coin.vol_24h) },
        { label:"Current Price",  value: coin.price != null ? `$${coin.price.toFixed(6)}` : "—" },
        { label:"Chain Count",    value: coin.chains || "—" },
        { label:"Regulatory",     value: coin.regulatory?.toUpperCase() || "None" },
        { label:"Attestation",    value: coin.attestation?.replace(/_/g, " ") || "None" },
        { label:"Peg Dev (avg)",  value: sub.peg_dev_avg != null ? `${sub.peg_dev_avg}%` : "—" },
        { label:"Peg Dev (max)",  value: sub.peg_dev_max != null ? `${sub.peg_dev_max}%` : "—" },
        { label:"Depeg Days",     value: sub.peg_depeg_days != null ? sub.peg_depeg_days : "—" },
        { label:"Spread (Binance)",value: sub.liq_spread_pct != null ? `${sub.liq_spread_pct.toFixed(3)}%` : "—" },
        { label:"Exchanges",      value: sub.liq_exchanges || "—" },
        { label:"TVL",            value: sub.liq_tvl ? `$${(sub.liq_tvl/1e6).toFixed(1)}M` : "—" },
        { label:"Age (yrs)",      value: coin.age_yrs || "—" },
        { label:"Has Pause Fn",  value: coin.has_pause ? "Yes" : "No" },
        { label:"Enforcement",   value: coin.enforcement?.replace(/_/g, " ") || "None" },
    ];
    document.getElementById("modalMetrics").innerHTML = metrics.map(m => `
        <div style="background:var(--bg-tertiary);padding:var(--space-3) var(--space-4);border-radius:var(--radius-sm);">
            <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:2px;">${m.label}</div>
            <div style="font-size:13px;font-weight:600;font-family:var(--font-mono);color:var(--text-primary);">${m.value}</div>
        </div>`).join("");

    // Flags
    const flags = coin.flags || [];
    if (flags.length) {
        document.getElementById("modalFlagsSection").style.display = "";
        document.getElementById("modalFlags").innerHTML = flags.map(f =>
            `<span class="flag" style="margin-right:6px;margin-bottom:4px;">${f.replace(/_/g, " ")}</span>`
        ).join("");
    } else {
        document.getElementById("modalFlagsSection").style.display = "none";
    }

    // Footer
    document.getElementById("modalFooter").textContent =
        `Score computed ${allData.run_id ? new Date(allData.run_id).toLocaleDateString("en-US", {dateStyle:"long",timeStyle:"short",timeZone:"UTC"}) + " UTC" : "—"} · v${allData.version || "1.1"}`;

    // Sparkline
    renderModalSparkline(coin);

    document.getElementById("modalOverlay").classList.add("open");
    document.body.style.overflow = "hidden";
}

let sparklineChart = null;
function renderModalSparkline(coin) {
    const canvas = document.getElementById("modalSparkline");
    const history = coin.sparkline || [];

    if (sparklineChart) { sparklineChart.destroy(); sparklineChart = null; }

    if (!history.length) {
        canvas.getContext("2d").clearRect(0, 0, canvas.width, canvas.height);
        return;
    }

    const color = gradeColor(coin.letter, "text");
    sparklineChart = new Chart(canvas, {
        type: "line",
        data: {
            labels: history.map((_, i) => i),
            datasets: [{
                data: history,
                borderColor: color,
                borderWidth: 1.5,
                fill: true,
                backgroundColor: color + "15",
                pointRadius: 0,
                tension: 0.3,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: { displayColors: false,
                callbacks: { title: () => "Recent Price", label: ctx => `$${ctx.parsed.y.toFixed(6)}` }
            }},
            scales: {
                x: { display: false },
                y: { display: false },
            },
            animation: { duration: 400 },
        },
    });
}

function initModal() {
    const overlay = document.getElementById("modalOverlay");
    const closeBtn = document.getElementById("modalClose");

    function close() {
        overlay.classList.remove("open");
        document.body.style.overflow = "";
        if (sparklineChart) { sparklineChart.destroy(); sparklineChart = null; }
    }

    closeBtn.addEventListener("click", close);
    overlay.addEventListener("click", e => { if (e.target === overlay) close(); });
    document.addEventListener("keydown", e => { if (e.key === "Escape") close(); });
}
