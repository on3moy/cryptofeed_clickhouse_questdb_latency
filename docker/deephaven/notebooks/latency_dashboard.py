import deephaven.ui as ui
from qdb_backend import create_live_table

DB_COLORS = {
    "QuestDB": "#00AA44",
    "ClickHouse": "#FF6600",
}

# Created once — not inside the component, so re-renders don't restart the backend
_metrics_table = create_live_table("latency_metrics", refreshing=True)


def _stats(data, key="latency_ms"):
    if not data or key not in data:
        return {"avg": 0, "p95": 0, "p99": 0, "min": 0, "max": 0, "n": 0}
    values = sorted([v for v in data[key] if v is not None])
    if not values:
        return {"avg": 0, "p95": 0, "p99": 0, "min": 0, "max": 0, "n": 0}
    n = len(values)
    return {
        "avg": sum(values) / n,
        "p95": values[int(n * 0.95)],
        "p99": values[int(n * 0.99)],
        "min": values[0],
        "max": values[-1],
        "n": n,
    }


def stat_card(label, stats, color):
    return ui.view(
        ui.heading(label, level=2, UNSAFE_style={"color": color}),
        ui.text(f"Avg:  {stats['avg']:.3f} ms"),
        ui.text(f"P95:  {stats['p95']:.3f} ms"),
        ui.text(f"P99:  {stats['p99']:.3f} ms"),
        ui.text(f"Min:  {stats['min']:.3f} ms"),
        ui.text(f"Max:  {stats['max']:.3f} ms"),
        ui.text(f"N:    {stats['n']} samples"),
        UNSAFE_style={
            "padding": "20px",
            "border": f"2px solid {color}",
            "borderRadius": "8px",
            "minWidth": "240px",
        },
    )


@ui.component
def latency_dashboard():
    operation, set_operation = ui.use_state("trade")
    window, set_window = ui.use_state(500)

    qdb_filtered  = ui.use_memo(lambda: _metrics_table.where(f"writer=`QuestDB` && operation=`{operation}`").tail(window), [operation, window])
    ch_filtered   = ui.use_memo(lambda: _metrics_table.where(f"writer=`ClickHouse` && operation=`{operation}`").tail(window), [operation, window])

    qdb_rows  = ui.use_table_data(qdb_filtered)
    ch_rows   = ui.use_table_data(ch_filtered)

    qdb_stats = _stats(qdb_rows)
    ch_stats  = _stats(ch_rows)

    return ui.view(
        ui.heading("DB Write Latency: QuestDB vs ClickHouse", level=1),

        # Controls
        ui.flex(
            ui.view(
                ui.text("Operation:", UNSAFE_style={"fontWeight": "bold", "marginRight": "8px"}),
                ui.button_group(
                    ui.button("trade",     on_press=lambda: set_operation("trade"),     variant="primary"    if operation == "trade"     else "secondary"),
                    ui.button("orderbook", on_press=lambda: set_operation("orderbook"), variant="primary"    if operation == "orderbook" else "secondary"),
                ),
                UNSAFE_style={"display": "flex", "alignItems": "center", "gap": "8px"},
            ),
            ui.view(
                ui.text("Window:", UNSAFE_style={"fontWeight": "bold", "marginRight": "8px"}),
                ui.button_group(
                    ui.button("100",  on_press=lambda: set_window(100),  variant="primary" if window == 100  else "secondary"),
                    ui.button("500",  on_press=lambda: set_window(500),  variant="primary" if window == 500  else "secondary"),
                    ui.button("1000", on_press=lambda: set_window(1000), variant="primary" if window == 1000 else "secondary"),
                ),
                UNSAFE_style={"display": "flex", "alignItems": "center", "gap": "8px"},
            ),
            direction="row",
            gap="40px",
            UNSAFE_style={"marginBottom": "24px"},
        ),

        # Stat cards side by side
        ui.flex(
            stat_card("QuestDB (ILP / TCP)",       qdb_stats, DB_COLORS["QuestDB"]),
            stat_card("ClickHouse (Native / TCP)",  ch_stats,  DB_COLORS["ClickHouse"]),
            direction="row",
            gap="24px",
            UNSAFE_style={"marginBottom": "24px"},
        ),

        # Live raw tables
        ui.flex(
            ui.view(
                ui.heading("QuestDB — live", level=4, UNSAFE_style={"color": DB_COLORS["QuestDB"]}),
                qdb_filtered,
                UNSAFE_style={"width": "50%", "height": "600px"},
            ),
            ui.view(
                ui.heading("ClickHouse — live", level=4, UNSAFE_style={"color": DB_COLORS["ClickHouse"]}),
                ch_filtered,
                UNSAFE_style={"width": "50%", "height": "600px"},
            ),
            direction="row",
            gap="16px",
        ),
    )


dashboard = ui.dashboard(latency_dashboard())
