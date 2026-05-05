#include <gtk/gtk.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer.h"
#include "producer.h"
#include "consumer.h"
#include "logger.h"

/* ─────────────────────────────────────────────
   Global simulation config (set from startup dialog)
   ───────────────────────────────────────────── */
static int cfg_buffer_size   = 10;
static int cfg_num_producers = 2;
static int cfg_num_consumers = 2;
static int cfg_items_limit   = 5;
static int cfg_sleep_delay   = 1;   // updated live by speed slider

volatile int stop_flag  = 0;        // set to 1 to stop all threads
volatile int pause_flag = 0;        // set to 1 to pause all threads

/* ─────────────────────────────────────────────
   Thread arrays (allocated after config is known)
   ───────────────────────────────────────────── */
static pthread_t*       producer_threads = NULL;
static pthread_t*       consumer_threads = NULL;
static producer_stats*  p_stats = NULL;
static thread_stats*    c_stats = NULL;

/* ─────────────────────────────────────────────
   GUI widget handles (needed in callbacks & refresh)
   ───────────────────────────────────────────── */
static GtkWidget* main_window      = NULL;
static GtkWidget* buffer_box       = NULL;   // HBox holding slot labels
static GtkWidget* log_textview     = NULL;
static GtkWidget* stats_total_prod = NULL;
static GtkWidget* stats_total_cons = NULL;
static GtkWidget* stats_in_buffer  = NULL;
static GtkWidget* stats_avg_wait   = NULL;
static GtkWidget* btn_start        = NULL;
static GtkWidget* btn_pause        = NULL;
static GtkWidget* btn_stop         = NULL;
static GtkWidget* speed_label      = NULL;

// Per-thread status labels (producers + consumers)
static GtkWidget** producer_status_labels = NULL;
static GtkWidget** producer_bar_labels    = NULL;
static GtkWidget** consumer_status_labels = NULL;
static GtkWidget** consumer_bar_labels    = NULL;

// Buffer slot colored boxes
static GtkWidget** slot_boxes = NULL;

static guint refresh_timer_id = 0;
static gboolean sim_running   = FALSE;

/* ─────────────────────────────────────────────
   CSS styling
   ───────────────────────────────────────────── */
static const char* APP_CSS =
    /* main window */
    "window { background-color: #0f1117; }"
    ".root-box { background-color: #0f1117; padding: 16px; }"

    /* top bar */
    ".topbar { background-color: #1a1d27; border-radius: 10px; padding: 10px 16px; margin-bottom: 14px; }"
    ".app-title { color: #e2e8f0; font-size: 15px; font-weight: bold; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".app-sub   { color: #64748b; font-size: 11px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"

    /* panels */
    ".panel { background-color: #1a1d27; border-radius: 10px; padding: 12px; margin: 4px; }"
    ".panel-title { color: #94a3b8; font-size: 10px; font-weight: bold; font-family: 'JetBrains Mono', 'Courier New', monospace; margin-bottom: 8px; }"

    /* buffer slots */
    ".slot-full  { background-color: #10b981; border-radius: 5px; min-width: 28px; min-height: 28px; }"
    ".slot-empty { background-color: #1e293b; border-radius: 5px; min-width: 28px; min-height: 28px; border: 1px solid #334155; }"
    ".slot-label { color: #e2e8f0; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"

    /* thread rows */
    ".thread-name  { color: #94a3b8; font-size: 11px; font-family: 'JetBrains Mono', 'Courier New', monospace; min-width: 80px; }"
    ".thread-count { color: #64748b; font-size: 10px; font-family: 'JetBrains Mono', 'Courier New', monospace; min-width: 36px; }"
    ".badge-run    { background-color: #064e3b; color: #10b981; border-radius: 8px; padding: 1px 7px; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".badge-wait   { background-color: #451a03; color: #f59e0b; border-radius: 8px; padding: 1px 7px; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".badge-done   { background-color: #1e293b; color: #475569; border-radius: 8px; padding: 1px 7px; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".badge-idle   { background-color: #1e293b; color: #475569; border-radius: 8px; padding: 1px 7px; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"

    /* progress bar track */
    ".bar-track { background-color: #0f172a; border-radius: 3px; min-height: 6px; }"
    ".bar-fill-p { background-color: #10b981; border-radius: 3px; min-height: 6px; }"
    ".bar-fill-c { background-color: #3b82f6; border-radius: 3px; min-height: 6px; }"

    /* stat cards */
    ".stat-card  { background-color: #0f172a; border-radius: 8px; padding: 8px 12px; margin: 3px; }"
    ".stat-value { color: #e2e8f0; font-size: 20px; font-weight: bold; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".stat-label { color: #475569; font-size: 9px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"

    /* log area */
    ".log-view { background-color: #0a0d14; color: #4ade80; font-size: 10px;"
    "            font-family: 'JetBrains Mono', 'Courier New', monospace; border-radius: 6px; }"
    "textview text { background-color: #0a0d14; color: #4ade80; }"

    /* control buttons */
    ".btn-start { background-color: #10b981; color: #fff; border-radius: 6px; padding: 6px 18px;"
    "             font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 11px; border: none; }"
    ".btn-start:hover { background-color: #059669; }"
    ".btn-pause { background-color: #f59e0b; color: #fff; border-radius: 6px; padding: 6px 18px;"
    "             font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 11px; border: none; }"
    ".btn-pause:hover { background-color: #d97706; }"
    ".btn-stop  { background-color: #ef4444; color: #fff; border-radius: 6px; padding: 6px 18px;"
    "             font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 11px; border: none; }"
    ".btn-stop:hover  { background-color: #dc2626; }"

    /* speed label */
    ".speed-val { color: #e2e8f0; font-size: 11px; font-family: 'JetBrains Mono', 'Courier New', monospace; min-width: 28px; }"
    ".speed-lbl { color: #64748b; font-size: 10px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"

    /* startup dialog */
    ".dialog-bg    { background-color: #0f1117; }"
    ".dialog-title { color: #e2e8f0; font-size: 18px; font-weight: bold; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".dialog-sub   { color: #64748b; font-size: 11px; font-family: 'JetBrains Mono', 'Courier New', monospace; margin-bottom: 14px; }"
    ".field-label  { color: #94a3b8; font-size: 11px; font-family: 'JetBrains Mono', 'Courier New', monospace; min-width: 160px; }"
    ".field-hint   { color: #334155; font-size: 10px; font-family: 'JetBrains Mono', 'Courier New', monospace; }"
    ".launch-btn   { background-color: #10b981; color: #fff; border-radius: 8px; padding: 10px 32px;"
    "                font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 12px; border: none; margin-top: 10px; }"
    ".launch-btn:hover { background-color: #059669; }"
    "spinbutton { background-color: #1a1d27; color: #e2e8f0; border-radius: 6px; border: 1px solid #334155;"
    "             font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 11px; }"
    "scale trough { background-color: #1e293b; border-radius: 4px; min-height: 4px; }"
    "scale highlight { background-color: #3b82f6; border-radius: 4px; }"
    "scale slider { background-color: #3b82f6; border-radius: 50%; min-width: 14px; min-height: 14px; border: none; }";

/* ─────────────────────────────────────────────
   Helper: append a line to the log textview (called from GTK main thread only)
   ───────────────────────────────────────────── */
typedef struct { char msg[256]; } LogMsg;

static gboolean append_log_idle(gpointer data) {
    LogMsg* lm = (LogMsg*)data;
    GtkTextBuffer* buf = gtk_text_view_get_buffer(GTK_TEXT_VIEW(log_textview));
    GtkTextIter end;
    gtk_text_buffer_get_end_iter(buf, &end);
    gtk_text_buffer_insert(buf, &end, lm->msg, -1);
    gtk_text_buffer_insert(buf, &end, "\n", -1);
    // auto-scroll to bottom
    GtkTextMark* mark = gtk_text_buffer_get_insert(buf);
    gtk_text_view_scroll_mark_onscreen(GTK_TEXT_VIEW(log_textview), mark);
    free(lm);
    return FALSE;
}

void gui_log(const char* msg) {
    // log_event already writes to file + stdout; we just push to GUI
    LogMsg* lm = malloc(sizeof(LogMsg));
    snprintf(lm->msg, sizeof(lm->msg), "%s", msg);
    g_idle_add(append_log_idle, lm);
}

/* ─────────────────────────────────────────────
   Refresh callback — fires every 500ms to update GUI
   ───────────────────────────────────────────── */
static gboolean refresh_gui(gpointer data) {
    if (!sim_running) return TRUE;

    // ── buffer slots ──────────────────────────
    pthread_mutex_lock(&lock);
    int current_count = count;
    int current_in    = in;
    pthread_mutex_unlock(&lock);

    for (int i = 0; i < cfg_buffer_size; i++) {
        GtkStyleContext* ctx = gtk_widget_get_style_context(slot_boxes[i]);
        // slot is "full" if it falls within the occupied range
        // simple heuristic: first current_count slots from 'out' are full
        int occupied = (i < current_count);
        if (occupied) {
            gtk_style_context_remove_class(ctx, "slot-empty");
            gtk_style_context_add_class(ctx, "slot-full");
        } else {
            gtk_style_context_remove_class(ctx, "slot-full");
            gtk_style_context_add_class(ctx, "slot-empty");
        }
    }

    // ── thread progress bars ──────────────────
    for (int i = 0; i < cfg_num_producers && p_stats; i++) {
        char count_str[16];
        snprintf(count_str, sizeof(count_str), "%d/%d",
                 p_stats[i].items_produced, cfg_items_limit);
        gtk_label_set_text(GTK_LABEL(producer_bar_labels[i]), count_str);

        // update status badge
        const char* badge_class;
        const char* badge_text;
        if (p_stats[i].items_produced >= cfg_items_limit) {
            badge_class = "badge-done"; badge_text = "done";
        } else if (stop_flag) {
            badge_class = "badge-done"; badge_text = "stopped";
        } else {
            badge_class = "badge-run";  badge_text = "running";
        }
        GtkStyleContext* ctx = gtk_widget_get_style_context(producer_status_labels[i]);
        gtk_style_context_remove_class(ctx, "badge-run");
        gtk_style_context_remove_class(ctx, "badge-wait");
        gtk_style_context_remove_class(ctx, "badge-done");
        gtk_style_context_remove_class(ctx, "badge-idle");
        gtk_style_context_add_class(ctx, badge_class);
        gtk_label_set_text(GTK_LABEL(producer_status_labels[i]), badge_text);
    }

    for (int i = 0; i < cfg_num_consumers && c_stats; i++) {
        char count_str[16];
        snprintf(count_str, sizeof(count_str), "%d/%d",
                 c_stats[i].items_consumed, cfg_items_limit);
        gtk_label_set_text(GTK_LABEL(consumer_bar_labels[i]), count_str);

        const char* badge_class;
        const char* badge_text;
        if (c_stats[i].items_consumed >= cfg_items_limit) {
            badge_class = "badge-done"; badge_text = "done";
        } else if (stop_flag) {
            badge_class = "badge-done"; badge_text = "stopped";
        } else {
            badge_class = "badge-run";  badge_text = "running";
        }
        GtkStyleContext* ctx = gtk_widget_get_style_context(consumer_status_labels[i]);
        gtk_style_context_remove_class(ctx, "badge-run");
        gtk_style_context_remove_class(ctx, "badge-wait");
        gtk_style_context_remove_class(ctx, "badge-done");
        gtk_style_context_remove_class(ctx, "badge-idle");
        gtk_style_context_add_class(ctx, badge_class);
        gtk_label_set_text(GTK_LABEL(consumer_status_labels[i]), badge_text);
    }

    // ── stat cards ────────────────────────────
    int total_produced = 0, total_consumed = 0;
    double total_wait = 0.0; int wait_count = 0;

    for (int i = 0; i < cfg_num_producers && p_stats; i++) {
        total_produced += p_stats[i].items_produced;
        if (p_stats[i].items_produced > 0) {
            total_wait  += p_stats[i].total_wait_time;
            wait_count  += p_stats[i].items_produced;
        }
    }
    for (int i = 0; i < cfg_num_consumers && c_stats; i++)
        total_consumed += c_stats[i].items_consumed;

    char tmp[32];
    snprintf(tmp, sizeof(tmp), "%d", total_produced);
    gtk_label_set_text(GTK_LABEL(stats_total_prod), tmp);
    snprintf(tmp, sizeof(tmp), "%d", total_consumed);
    gtk_label_set_text(GTK_LABEL(stats_total_cons), tmp);
    snprintf(tmp, sizeof(tmp), "%d", current_count);
    gtk_label_set_text(GTK_LABEL(stats_in_buffer), tmp);
    if (wait_count > 0)
        snprintf(tmp, sizeof(tmp), "%.2fs", total_wait / wait_count);
    else
        snprintf(tmp, sizeof(tmp), "—");
    gtk_label_set_text(GTK_LABEL(stats_avg_wait), tmp);

    return TRUE;  // keep timer running
}

/* ─────────────────────────────────────────────
   Button callbacks
   ───────────────────────────────────────────── */
static void on_start_clicked(GtkButton* btn, gpointer data) {
    if (sim_running) return;
    sim_running = TRUE;
    stop_flag   = 0;
    pause_flag  = 0;

    // allocate stats structs
    p_stats = calloc(cfg_num_producers, sizeof(producer_stats));
    c_stats = calloc(cfg_num_consumers, sizeof(thread_stats));

    // fill stats fields
    for (int i = 0; i < cfg_num_producers; i++) {
        p_stats[i].thread_id   = i + 1;
        p_stats[i].items_limit = cfg_items_limit;
        p_stats[i].sleep_delay = cfg_sleep_delay;
    }
    for (int i = 0; i < cfg_num_consumers; i++) {
        c_stats[i].thread_id   = i + 1;
        c_stats[i].items_limit = cfg_items_limit;
        c_stats[i].sleep_delay = cfg_sleep_delay;
    }

    // spawn threads
    producer_threads = malloc(cfg_num_producers * sizeof(pthread_t));
    consumer_threads = malloc(cfg_num_consumers * sizeof(pthread_t));
    for (int i = 0; i < cfg_num_producers; i++)
        pthread_create(&producer_threads[i], NULL, producer, &p_stats[i]);
    for (int i = 0; i < cfg_num_consumers; i++)
        pthread_create(&consumer_threads[i], NULL, consumer, &c_stats[i]);

    log_event("Simulation started.");
    gtk_widget_set_sensitive(btn_start, FALSE);
    gtk_widget_set_sensitive(btn_pause, TRUE);
    gtk_widget_set_sensitive(btn_stop,  TRUE);
}

static void on_pause_clicked(GtkButton* btn, gpointer data) {
    pause_flag = !pause_flag;
    if (pause_flag) {
        gtk_button_set_label(GTK_BUTTON(btn_pause), "Resume");
        log_event("Simulation paused.");
    } else {
        gtk_button_set_label(GTK_BUTTON(btn_pause), "Pause");
        log_event("Simulation resumed.");
    }
}

/* background thread that joins all sim threads then re-enables UI */
static void* join_thread_worker(void* arg) {
    for (int i = 0; i < cfg_num_producers; i++)
        pthread_join(producer_threads[i], NULL);
    for (int i = 0; i < cfg_num_consumers; i++)
        pthread_join(consumer_threads[i], NULL);

    free(producer_threads); producer_threads = NULL;
    free(consumer_threads); consumer_threads = NULL;

    // schedule GUI updates back on the GTK main thread
    g_idle_add((GSourceFunc)log_event, "All threads finished. Simulation stopped.");
    g_idle_add_full(G_PRIORITY_DEFAULT, (GSourceFunc)gtk_widget_set_sensitive,
                    btn_start, NULL);   // re-enable start
    return NULL;
}

static gboolean reenable_buttons(gpointer data) {
    gtk_widget_set_sensitive(btn_start, TRUE);
    gtk_widget_set_sensitive(btn_pause, FALSE);
    gtk_widget_set_sensitive(btn_stop,  FALSE);
    gtk_button_set_label(GTK_BUTTON(btn_pause), "Pause");
    return FALSE;   // run once
}

static void on_stop_clicked(GtkButton* btn, gpointer data) {
    stop_flag   = 1;
    pause_flag  = 0;
    sim_running = FALSE;
    log_event("Stop signal sent — waiting for threads to finish...");

    // disable buttons immediately so user can't click again
    gtk_widget_set_sensitive(btn_start, FALSE);
    gtk_widget_set_sensitive(btn_pause, FALSE);
    gtk_widget_set_sensitive(btn_stop,  FALSE);

    // join threads in a background worker so the GTK main thread is never blocked
    pthread_t joiner;
    pthread_create(&joiner, NULL, join_thread_worker, NULL);
    pthread_detach(joiner);     // we don't need to join the joiner itself

    // re-enable buttons after a short delay (threads finish within sleep_delay + margin)
    g_timeout_add(500, reenable_buttons, NULL);
}

static void on_speed_changed(GtkRange* range, gpointer data) {
    // slider value: 1 (fast) to 5 (slow)
    int val = (int)gtk_range_get_value(range);
    cfg_sleep_delay = val;

    // update all running thread stats live
    for (int i = 0; i < cfg_num_producers && p_stats; i++)
        p_stats[i].sleep_delay = val;
    for (int i = 0; i < cfg_num_consumers && c_stats; i++)
        c_stats[i].sleep_delay = val;

    char lbl[16];
    snprintf(lbl, sizeof(lbl), "%ds", val);
    gtk_label_set_text(GTK_LABEL(speed_label), lbl);
}

/* ─────────────────────────────────────────────
   Build the main simulation window (Option B layout)
   ───────────────────────────────────────────── */
static void build_main_window(void) {
    main_window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
    gtk_window_set_title(GTK_WINDOW(main_window), "Producer-Consumer Simulator");
    gtk_window_set_default_size(GTK_WINDOW(main_window), 860, 620);
    gtk_window_set_resizable(GTK_WINDOW(main_window), TRUE);
    g_signal_connect(main_window, "destroy", G_CALLBACK(gtk_main_quit), NULL);

    // ── root vertical box ─────────────────────
    GtkWidget* root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_style_context_add_class(gtk_widget_get_style_context(root), "root-box");
    gtk_container_add(GTK_CONTAINER(main_window), root);

    // ── top bar ───────────────────────────────
    GtkWidget* topbar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 12);
    gtk_style_context_add_class(gtk_widget_get_style_context(topbar), "topbar");
    gtk_box_pack_start(GTK_BOX(root), topbar, FALSE, FALSE, 0);

    GtkWidget* title_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
    GtkWidget* title_lbl = gtk_label_new("PRODUCER-CONSUMER SIMULATOR");
    gtk_style_context_add_class(gtk_widget_get_style_context(title_lbl), "app-title");
    gtk_label_set_xalign(GTK_LABEL(title_lbl), 0);
    char sub_str[128];
    snprintf(sub_str, sizeof(sub_str),
             "buf:%d  producers:%d  consumers:%d  items/thread:%d",
             cfg_buffer_size, cfg_num_producers, cfg_num_consumers, cfg_items_limit);
    GtkWidget* sub_lbl = gtk_label_new(sub_str);
    gtk_style_context_add_class(gtk_widget_get_style_context(sub_lbl), "app-sub");
    gtk_label_set_xalign(GTK_LABEL(sub_lbl), 0);
    gtk_box_pack_start(GTK_BOX(title_col), title_lbl, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(title_col), sub_lbl,   FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(topbar), title_col, TRUE, TRUE, 0);

    // controls in topbar
    btn_start = gtk_button_new_with_label("Start");
    btn_pause = gtk_button_new_with_label("Pause");
    btn_stop  = gtk_button_new_with_label("Stop");
    gtk_style_context_add_class(gtk_widget_get_style_context(btn_start), "btn-start");
    gtk_style_context_add_class(gtk_widget_get_style_context(btn_pause), "btn-pause");
    gtk_style_context_add_class(gtk_widget_get_style_context(btn_stop),  "btn-stop");
    gtk_widget_set_sensitive(btn_pause, FALSE);
    gtk_widget_set_sensitive(btn_stop,  FALSE);
    g_signal_connect(btn_start, "clicked", G_CALLBACK(on_start_clicked), NULL);
    g_signal_connect(btn_pause, "clicked", G_CALLBACK(on_pause_clicked), NULL);
    g_signal_connect(btn_stop,  "clicked", G_CALLBACK(on_stop_clicked),  NULL);
    gtk_box_pack_start(GTK_BOX(topbar), btn_start, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(topbar), btn_pause, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(topbar), btn_stop,  FALSE, FALSE, 0);

    // speed slider
    GtkWidget* speed_lbl_static = gtk_label_new("Speed");
    gtk_style_context_add_class(gtk_widget_get_style_context(speed_lbl_static), "speed-lbl");
    GtkWidget* speed_scale = gtk_scale_new_with_range(GTK_ORIENTATION_HORIZONTAL, 1, 5, 1);
    gtk_range_set_value(GTK_RANGE(speed_scale), cfg_sleep_delay);
    gtk_scale_set_draw_value(GTK_SCALE(speed_scale), FALSE);
    gtk_widget_set_size_request(speed_scale, 100, -1);
    speed_label = gtk_label_new("1s");
    gtk_style_context_add_class(gtk_widget_get_style_context(speed_label), "speed-val");
    g_signal_connect(speed_scale, "value-changed", G_CALLBACK(on_speed_changed), NULL);
    gtk_box_pack_start(GTK_BOX(topbar), speed_lbl_static, FALSE, FALSE, 6);
    gtk_box_pack_start(GTK_BOX(topbar), speed_scale,      FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(topbar), speed_label,      FALSE, FALSE, 0);

    // ── main content: left (2fr) + right sidebar ──
    GtkWidget* content = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_box_pack_start(GTK_BOX(root), content, TRUE, TRUE, 0);

    // ── LEFT COLUMN ───────────────────────────
    GtkWidget* left_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_box_pack_start(GTK_BOX(content), left_col, TRUE, TRUE, 0);

    // Buffer panel
    GtkWidget* buf_panel = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);
    gtk_style_context_add_class(gtk_widget_get_style_context(buf_panel), "panel");
    gtk_box_pack_start(GTK_BOX(left_col), buf_panel, FALSE, FALSE, 0);

    char buf_title[64];
    snprintf(buf_title, sizeof(buf_title), "ORDER TABLE  (%d slots)", cfg_buffer_size);
    GtkWidget* buf_title_lbl = gtk_label_new(buf_title);
    gtk_style_context_add_class(gtk_widget_get_style_context(buf_title_lbl), "panel-title");
    gtk_label_set_xalign(GTK_LABEL(buf_title_lbl), 0);
    gtk_box_pack_start(GTK_BOX(buf_panel), buf_title_lbl, FALSE, FALSE, 0);

    // slot boxes row
    buffer_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 4);
    slot_boxes = malloc(cfg_buffer_size * sizeof(GtkWidget*));
    for (int i = 0; i < cfg_buffer_size; i++) {
        GtkWidget* slot = gtk_label_new("");
        gtk_style_context_add_class(gtk_widget_get_style_context(slot), "slot-empty");
        gtk_widget_set_size_request(slot, 28, 28);
        slot_boxes[i] = slot;
        gtk_box_pack_start(GTK_BOX(buffer_box), slot, FALSE, FALSE, 0);
    }
    gtk_box_pack_start(GTK_BOX(buf_panel), buffer_box, FALSE, FALSE, 0);

    // Producers panel
    GtkWidget* prod_panel = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    gtk_style_context_add_class(gtk_widget_get_style_context(prod_panel), "panel");
    gtk_box_pack_start(GTK_BOX(left_col), prod_panel, FALSE, FALSE, 0);

    GtkWidget* prod_title = gtk_label_new("WAITERS  (producers)");
    gtk_style_context_add_class(gtk_widget_get_style_context(prod_title), "panel-title");
    gtk_label_set_xalign(GTK_LABEL(prod_title), 0);
    gtk_box_pack_start(GTK_BOX(prod_panel), prod_title, FALSE, FALSE, 0);

    producer_status_labels = malloc(cfg_num_producers * sizeof(GtkWidget*));
    producer_bar_labels    = malloc(cfg_num_producers * sizeof(GtkWidget*));
    for (int i = 0; i < cfg_num_producers; i++) {
        GtkWidget* row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
        char name[24]; snprintf(name, sizeof(name), "Waiter %d", i + 1);
        GtkWidget* name_lbl = gtk_label_new(name);
        gtk_style_context_add_class(gtk_widget_get_style_context(name_lbl), "thread-name");
        gtk_label_set_xalign(GTK_LABEL(name_lbl), 0);

        GtkWidget* badge = gtk_label_new("idle");
        gtk_style_context_add_class(gtk_widget_get_style_context(badge), "badge-idle");
        producer_status_labels[i] = badge;

        GtkWidget* bar_bg = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
        gtk_style_context_add_class(gtk_widget_get_style_context(bar_bg), "bar-track");
        gtk_widget_set_size_request(bar_bg, -1, 6);

        GtkWidget* count_lbl = gtk_label_new("0/0");
        gtk_style_context_add_class(gtk_widget_get_style_context(count_lbl), "thread-count");
        producer_bar_labels[i] = count_lbl;

        gtk_box_pack_start(GTK_BOX(row), name_lbl,  FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(row), badge,     FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(row), bar_bg,    TRUE,  TRUE,  0);
        gtk_box_pack_start(GTK_BOX(row), count_lbl, FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(prod_panel), row, FALSE, FALSE, 0);
    }

    // Consumers panel
    GtkWidget* cons_panel = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    gtk_style_context_add_class(gtk_widget_get_style_context(cons_panel), "panel");
    gtk_box_pack_start(GTK_BOX(left_col), cons_panel, FALSE, FALSE, 0);

    GtkWidget* cons_title = gtk_label_new("CHEFS  (consumers)");
    gtk_style_context_add_class(gtk_widget_get_style_context(cons_title), "panel-title");
    gtk_label_set_xalign(GTK_LABEL(cons_title), 0);
    gtk_box_pack_start(GTK_BOX(cons_panel), cons_title, FALSE, FALSE, 0);

    consumer_status_labels = malloc(cfg_num_consumers * sizeof(GtkWidget*));
    consumer_bar_labels    = malloc(cfg_num_consumers * sizeof(GtkWidget*));
    for (int i = 0; i < cfg_num_consumers; i++) {
        GtkWidget* row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
        char name[24]; snprintf(name, sizeof(name), "Chef %d", i + 1);
        GtkWidget* name_lbl = gtk_label_new(name);
        gtk_style_context_add_class(gtk_widget_get_style_context(name_lbl), "thread-name");
        gtk_label_set_xalign(GTK_LABEL(name_lbl), 0);

        GtkWidget* badge = gtk_label_new("idle");
        gtk_style_context_add_class(gtk_widget_get_style_context(badge), "badge-idle");
        consumer_status_labels[i] = badge;

        GtkWidget* bar_bg = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
        gtk_style_context_add_class(gtk_widget_get_style_context(bar_bg), "bar-track");
        gtk_widget_set_size_request(bar_bg, -1, 6);

        GtkWidget* count_lbl = gtk_label_new("0/0");
        gtk_style_context_add_class(gtk_widget_get_style_context(count_lbl), "thread-count");
        consumer_bar_labels[i] = count_lbl;

        gtk_box_pack_start(GTK_BOX(row), name_lbl,  FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(row), badge,     FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(row), bar_bg,    TRUE,  TRUE,  0);
        gtk_box_pack_start(GTK_BOX(row), count_lbl, FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(cons_panel), row, FALSE, FALSE, 0);
    }

    // Log panel
    GtkWidget* log_panel = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);
    gtk_style_context_add_class(gtk_widget_get_style_context(log_panel), "panel");
    gtk_box_pack_start(GTK_BOX(left_col), log_panel, TRUE, TRUE, 0);

    GtkWidget* log_title = gtk_label_new("LIVE LOG");
    gtk_style_context_add_class(gtk_widget_get_style_context(log_title), "panel-title");
    gtk_label_set_xalign(GTK_LABEL(log_title), 0);
    gtk_box_pack_start(GTK_BOX(log_panel), log_title, FALSE, FALSE, 0);

    log_textview = gtk_text_view_new();
    gtk_text_view_set_editable(GTK_TEXT_VIEW(log_textview), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(log_textview), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(log_textview), GTK_WRAP_WORD_CHAR);
    gtk_style_context_add_class(gtk_widget_get_style_context(log_textview), "log-view");

    GtkWidget* log_scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(log_scroll),
                                   GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
    gtk_container_add(GTK_CONTAINER(log_scroll), log_textview);
    gtk_widget_set_size_request(log_scroll, -1, 120);
    gtk_box_pack_start(GTK_BOX(log_panel), log_scroll, TRUE, TRUE, 0);

    // ── RIGHT SIDEBAR ─────────────────────────
    GtkWidget* right_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_widget_set_size_request(right_col, 160, -1);
    gtk_box_pack_start(GTK_BOX(content), right_col, FALSE, FALSE, 0);

    // 4 stat cards
    struct { const char* label; GtkWidget** widget_ptr; } cards[] = {
        { "total produced", &stats_total_prod },
        { "total consumed", &stats_total_cons },
        { "in buffer",      &stats_in_buffer  },
        { "avg wait time",  &stats_avg_wait   },
    };
    for (int i = 0; i < 4; i++) {
        GtkWidget* card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
        gtk_style_context_add_class(gtk_widget_get_style_context(card), "stat-card");
        GtkWidget* val = gtk_label_new("0");
        gtk_style_context_add_class(gtk_widget_get_style_context(val), "stat-value");
        gtk_label_set_xalign(GTK_LABEL(val), 0);
        GtkWidget* lbl = gtk_label_new(cards[i].label);
        gtk_style_context_add_class(gtk_widget_get_style_context(lbl), "stat-label");
        gtk_label_set_xalign(GTK_LABEL(lbl), 0);
        gtk_box_pack_start(GTK_BOX(card), val, FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(card), lbl, FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(right_col), card, FALSE, FALSE, 0);
        *cards[i].widget_ptr = val;
    }

    // start 500ms refresh timer
    refresh_timer_id = g_timeout_add(500, refresh_gui, NULL);

    gtk_widget_show_all(main_window);
}

/* ─────────────────────────────────────────────
   Startup dialog
   ───────────────────────────────────────────── */
static GtkWidget* spin_buf_size   = NULL;
static GtkWidget* spin_producers  = NULL;
static GtkWidget* spin_consumers  = NULL;
static GtkWidget* spin_items      = NULL;
static GtkWidget* startup_dialog  = NULL;

static void on_launch_clicked(GtkButton* btn, gpointer data) {
    cfg_buffer_size   = (int)gtk_spin_button_get_value(GTK_SPIN_BUTTON(spin_buf_size));
    cfg_num_producers = (int)gtk_spin_button_get_value(GTK_SPIN_BUTTON(spin_producers));
    cfg_num_consumers = (int)gtk_spin_button_get_value(GTK_SPIN_BUTTON(spin_consumers));
    cfg_items_limit   = (int)gtk_spin_button_get_value(GTK_SPIN_BUTTON(spin_items));
    cfg_sleep_delay   = 1;

    // initialize buffer with chosen size
    init(cfg_buffer_size);
    log_init("activity_log.txt");

    // IMPORTANT: disconnect destroy→gtk_main_quit BEFORE destroying the dialog,
    // otherwise destroying it fires gtk_main_quit and kills the whole app
    g_signal_handlers_disconnect_by_func(startup_dialog,
                                         G_CALLBACK(gtk_main_quit), NULL);
    gtk_widget_destroy(startup_dialog);
    startup_dialog = NULL;

    build_main_window();
}

static void build_startup_dialog(void) {
    startup_dialog = gtk_window_new(GTK_WINDOW_TOPLEVEL);
    gtk_window_set_title(GTK_WINDOW(startup_dialog), "Configure Simulation");
    gtk_window_set_default_size(GTK_WINDOW(startup_dialog), 400, 340);
    gtk_window_set_resizable(GTK_WINDOW(startup_dialog), FALSE);
    gtk_window_set_position(GTK_WINDOW(startup_dialog), GTK_WIN_POS_CENTER);
    g_signal_connect(startup_dialog, "destroy", G_CALLBACK(gtk_main_quit), NULL);

    GtkWidget* root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_style_context_add_class(gtk_widget_get_style_context(root), "root-box");
    gtk_container_add(GTK_CONTAINER(startup_dialog), root);

    GtkWidget* inner = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_start(inner, 24);
    gtk_widget_set_margin_end(inner, 24);
    gtk_widget_set_margin_top(inner, 24);
    gtk_widget_set_margin_bottom(inner, 24);
    gtk_box_pack_start(GTK_BOX(root), inner, TRUE, TRUE, 0);

    GtkWidget* title = gtk_label_new("SIMULATION CONFIG");
    gtk_style_context_add_class(gtk_widget_get_style_context(title), "dialog-title");
    gtk_label_set_xalign(GTK_LABEL(title), 0);
    gtk_box_pack_start(GTK_BOX(inner), title, FALSE, FALSE, 0);

    GtkWidget* sub = gtk_label_new("Producer-Consumer Problem Simulator");
    gtk_style_context_add_class(gtk_widget_get_style_context(sub), "dialog-sub");
    gtk_label_set_xalign(GTK_LABEL(sub), 0);
    gtk_box_pack_start(GTK_BOX(inner), sub, FALSE, FALSE, 0);

    // helper macro: one row with a label + spin button + hint
    struct {
        const char* label;
        const char* hint;
        double      min, max, val;
        GtkWidget** spin_ptr;
    } fieldgits[] = {
        { "Buffer size",         "5 – 50 slots",     5,  50, 10, &spin_buf_size  },
        { "Producer threads",    "1 – 10 waiters",   1,  10,  2, &spin_producers },
        { "Consumer threads",    "1 – 10 chefs",     1,  10,  2, &spin_consumers },
        { "Items per thread",    "1 – 50 orders",    1,  50,  5, &spin_items     },
    };
    for (int i = 0; i < 4; i++) {
        GtkWidget* row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
        GtkWidget* lbl_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
        GtkWidget* lbl  = gtk_label_new(fields[i].label);
        gtk_style_context_add_class(gtk_widget_get_style_context(lbl), "field-label");
        gtk_label_set_xalign(GTK_LABEL(lbl), 0);
        GtkWidget* hint = gtk_label_new(fields[i].hint);
        gtk_style_context_add_class(gtk_widget_get_style_context(hint), "field-hint");
        gtk_label_set_xalign(GTK_LABEL(hint), 0);
        gtk_box_pack_start(GTK_BOX(lbl_col), lbl,  FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(lbl_col), hint, FALSE, FALSE, 0);

        GtkWidget* spin = gtk_spin_button_new_with_range(fields[i].min, fields[i].max, 1);
        gtk_spin_button_set_value(GTK_SPIN_BUTTON(spin), fields[i].val);
        gtk_widget_set_size_request(spin, 80, -1);
        *fields[i].spin_ptr = spin;

        gtk_box_pack_start(GTK_BOX(row), lbl_col, TRUE,  TRUE,  0);
        gtk_box_pack_start(GTK_BOX(row), spin,    FALSE, FALSE, 0);
        gtk_box_pack_start(GTK_BOX(inner), row, FALSE, FALSE, 0);
    }

    GtkWidget* launch = gtk_button_new_with_label("Launch Simulation →");
    gtk_style_context_add_class(gtk_widget_get_style_context(launch), "launch-btn");
    g_signal_connect(launch, "clicked", G_CALLBACK(on_launch_clicked), NULL);
    gtk_box_pack_start(GTK_BOX(inner), launch, FALSE, FALSE, 0);

    gtk_widget_show_all(startup_dialog);
}

/* ─────────────────────────────────────────────
   main()
   ───────────────────────────────────────────── */
int main(int argc, char* argv[]) {
    gtk_init(&argc, &argv);

    // apply CSS theme
    GtkCssProvider* css = gtk_css_provider_new();
    gtk_css_provider_load_from_data(css, APP_CSS, -1, NULL);
    gtk_style_context_add_provider_for_screen(
        gdk_screen_get_default(),
        GTK_STYLE_PROVIDER(css),
        GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);

    build_startup_dialog();
    gtk_main();

    // cleanup on exit
    destroy();
    log_close();
    free(slot_boxes);
    free(producer_status_labels);
    free(producer_bar_labels);
    free(consumer_status_labels);
    free(consumer_bar_labels);
    free(p_stats);
    free(c_stats);

    return 0;
}