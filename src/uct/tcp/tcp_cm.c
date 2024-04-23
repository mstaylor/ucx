/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "tcp.h"
#include "tcp/tcp.h"
#include "tcp/nat_traversal.h"
#include "tcp/redis.h"
#include <poll.h>

#include <ucs/async/async.h>


void uct_tcp_cm_change_conn_state(uct_tcp_ep_t *ep,
                                  uct_tcp_ep_conn_state_t new_conn_state)
{
    int full_log           = 1;
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    char str_local_addr[UCS_SOCKADDR_STRING_LEN];
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    char str_ctx_caps[UCT_TCP_EP_CTX_CAPS_STR_MAX];
    uct_tcp_ep_conn_state_t old_conn_state;

    old_conn_state = (uct_tcp_ep_conn_state_t)ep->conn_state;
    ep->conn_state = new_conn_state;

    switch(ep->conn_state) {
    case UCT_TCP_EP_CONN_STATE_CONNECTING:
    case UCT_TCP_EP_CONN_STATE_WAITING_ACK:
        if (old_conn_state == UCT_TCP_EP_CONN_STATE_CLOSED) {
            uct_tcp_iface_outstanding_inc(iface);
        } else {
            ucs_assert((ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING) ||
                       (old_conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING));
        }
        break;
    case UCT_TCP_EP_CONN_STATE_CONNECTED:
        /* old_conn_state could be CONNECTING may happen when a peer is going
         * to use this EP with socket from accepted connection in case of
         * handling simultaneous connection establishment */
        ucs_assert(((old_conn_state == UCT_TCP_EP_CONN_STATE_CLOSED) &&
                    (ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP))         ||
                   (old_conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING)  ||
                   (old_conn_state == UCT_TCP_EP_CONN_STATE_WAITING_ACK) ||
                   (old_conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING));
        if (old_conn_state != UCT_TCP_EP_CONN_STATE_CLOSED) {
            /* Decrement iface's outstanding counter only in case of the
             * previous state is not CLOSED. If it is CLOSED it means that
             * iface's outstanding counter wasn't incremented prior */
            uct_tcp_iface_outstanding_dec(iface);
        }

        if (ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_TX) {
            /* Progress possibly pending TX operations */
            uct_tcp_ep_pending_queue_dispatch(ep);
        }
        break;
    case UCT_TCP_EP_CONN_STATE_CLOSED:
        ucs_assert(ep->events == 0);
        if (old_conn_state == UCT_TCP_EP_CONN_STATE_CLOSED) {
            return;
        }

        if ((old_conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING) ||
            (old_conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING) ||
            (old_conn_state == UCT_TCP_EP_CONN_STATE_WAITING_ACK)) {
            uct_tcp_iface_outstanding_dec(iface);
        }

        if ((old_conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING) ||
            (old_conn_state == UCT_TCP_EP_CONN_STATE_RECV_MAGIC_NUMBER)) {
            /* Since ep::peer_addr is 0'ed, we have to print w/o peer's address */
            full_log = 0;
        }
        break;
    case UCT_TCP_EP_CONN_STATE_ACCEPTING:
        ucs_assert((old_conn_state == UCT_TCP_EP_CONN_STATE_RECV_MAGIC_NUMBER) ||
                   ((old_conn_state == UCT_TCP_EP_CONN_STATE_CLOSED) &&
                    (ep->conn_retries == 0) &&
                    (ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP)));
        uct_tcp_iface_outstanding_inc(iface);
        /* fall through */
    default:
        ucs_assert((ep->conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING) ||
                   (ep->conn_state == UCT_TCP_EP_CONN_STATE_RECV_MAGIC_NUMBER));
        /* Since ep::peer_addr is 0'ed and client's <address:port>
         * has already been logged, print w/o peer's address */
        full_log = 0;
        break;
    }

    if (full_log) {
        ucs_debug("tcp_ep %p: %s -> %s for the [%s]<->[%s]:%"PRIu64" connection %s",
                  ep, uct_tcp_ep_cm_state[old_conn_state].name,
                  uct_tcp_ep_cm_state[ep->conn_state].name,
                  ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                                   str_local_addr, UCS_SOCKADDR_STRING_LEN),
                  ucs_sockaddr_str((const struct sockaddr*)&ep->peer_addr,
                                   str_remote_addr, UCS_SOCKADDR_STRING_LEN),
                  uct_tcp_ep_get_cm_id(ep),
                  uct_tcp_ep_ctx_caps_str(ep->flags, str_ctx_caps));
    } else {
        ucs_debug("tcp_ep %p: %s -> %s",
                  ep, uct_tcp_ep_cm_state[old_conn_state].name,
                  uct_tcp_ep_cm_state[ep->conn_state].name);
    }
}

/* `fmt_str` parameter has to contain "%s" to write event type */
static void uct_tcp_cm_trace_conn_pkt(const uct_tcp_ep_t *ep,
                                      ucs_log_level_t log_level,
                                      const char *fmt_str,
                                      uct_tcp_cm_conn_event_t event)
{
    UCS_STRING_BUFFER_ONSTACK(strb, 128);
    char str_addr[UCS_SOCKADDR_STRING_LEN];

    if (event == UCT_TCP_CM_CONN_FIN) {
        ucs_string_buffer_appendf(&strb, "%s",
                                  UCS_PP_MAKE_STRING(UCT_TCP_CM_CONN_FIN));
    } else if ((event & ~(UCT_TCP_CM_CONN_REQ | UCT_TCP_CM_CONN_ACK)) == 0) {
        ucs_string_buffer_appendf(&strb, "UNKNOWN (%d)", event);
    } else {
        if (event & UCT_TCP_CM_CONN_REQ) {
            ucs_string_buffer_appendf(&strb, "%s",
                                      UCS_PP_MAKE_STRING(UCT_TCP_CM_CONN_REQ));
        }

        if (event & UCT_TCP_CM_CONN_ACK) {
            ucs_string_buffer_appendf(&strb, "%s%s",
                                      ucs_string_buffer_length(&strb) ?
                                      " | " : "",
                                      UCS_PP_MAKE_STRING(UCT_TCP_CM_CONN_ACK));
        }
    }

    ucs_warn("tcp_ep %p: %s [%s]:%"PRIu64, ep,
            ucs_string_buffer_cstr(&strb),
            ucs_sockaddr_str((const struct sockaddr*)&ep->peer_addr,
                             str_addr, UCS_SOCKADDR_STRING_LEN),
            uct_tcp_ep_get_cm_id(ep));
}

ucs_status_t uct_tcp_cm_send_event_pending_cb(uct_pending_req_t *self)
{
    uct_tcp_ep_pending_req_t *req =
            ucs_derived_of(self, uct_tcp_ep_pending_req_t);
    ucs_status_t UCS_V_UNUSED status;

    status = uct_tcp_cm_send_event(req->ep, req->cm.event, req->cm.log_error);
    ucs_assert((status != UCS_INPROGRESS) && (status != UCS_ERR_NO_RESOURCE));

    ucs_free(req);
    return UCS_OK;
}

static ucs_status_t uct_tcp_cm_event_pending_add(uct_tcp_ep_t *ep,
                                                 uct_tcp_cm_conn_event_t event,
                                                 int log_error)
{
    uct_tcp_ep_pending_req_t *req;
    ucs_status_t UCS_V_UNUSED status;

    req = ucs_malloc(sizeof(*req), "tcp_cm_event_pending_req");
    if (ucs_unlikely(req == NULL)) {
        return UCS_ERR_NO_MEMORY;
    }

    req->ep           = ep;
    req->cm.event     = event;
    req->cm.log_error = log_error;
    req->super.func   = uct_tcp_cm_send_event_pending_cb;

    status = uct_tcp_ep_pending_add(&ep->super.super, &req->super, 0);
    ucs_assertv(status == UCS_OK, "ep %p: pending_add status: %d", ep,
                status);

    return UCS_OK;
}

ucs_status_t uct_tcp_cm_send_event(uct_tcp_ep_t *ep,
                                   uct_tcp_cm_conn_event_t event,
                                   int log_error)
{
    uct_tcp_iface_t *iface     = ucs_derived_of(ep->super.super.iface,
                                                uct_tcp_iface_t);
    size_t magic_number_length = 0;
    void *pkt_buf;
    size_t pkt_length, cm_pkt_length;
    uct_tcp_cm_conn_req_pkt_t *conn_pkt;
    uct_tcp_cm_conn_event_t *pkt_event;
    uct_tcp_am_hdr_t *pkt_hdr;
    ucs_status_t status;

    ucs_assertv(!(event & ~(UCT_TCP_CM_CONN_REQ |
                            UCT_TCP_CM_CONN_ACK |
                            UCT_TCP_CM_CONN_FIN)),
                "ep=%p", ep);

    if (!uct_tcp_ep_ctx_buf_empty(&ep->tx)) {
        return uct_tcp_cm_event_pending_add(ep, event, log_error);
    }

    pkt_length                  = sizeof(*pkt_hdr);
    if (event == UCT_TCP_CM_CONN_REQ) {
        cm_pkt_length = sizeof(*conn_pkt) + iface->config.sockaddr_len;

        if (ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING) {
            magic_number_length = sizeof(uint64_t);
        }
    } else {
        cm_pkt_length           = sizeof(event);
    }

    pkt_length     += cm_pkt_length + magic_number_length;
    pkt_buf         = ucs_alloca(pkt_length);
    pkt_hdr         = (uct_tcp_am_hdr_t*)(UCS_PTR_BYTE_OFFSET(pkt_buf,
                                                              magic_number_length));
    pkt_hdr->am_id  = UCT_TCP_EP_CM_AM_ID;
    pkt_hdr->length = cm_pkt_length;

    if (event == UCT_TCP_CM_CONN_REQ) {
        if (ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING) {
            ucs_assert(magic_number_length == sizeof(uint64_t));
            *(uint64_t*)pkt_buf = UCT_TCP_MAGIC_NUMBER;
        }

        conn_pkt        = (uct_tcp_cm_conn_req_pkt_t*)(pkt_hdr + 1);
        conn_pkt->event = UCT_TCP_CM_CONN_REQ;
        conn_pkt->flags = (ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP) ?
                          UCT_TCP_CM_CONN_REQ_PKT_FLAG_CONNECT_TO_EP : 0;
        conn_pkt->cm_id = ep->cm_id;
        memcpy(conn_pkt + 1, &iface->config.ifaddr, iface->config.sockaddr_len);
    } else {
        /* CM events (except CONN_REQ) are not sent for EPs connected with
         * CONNECT_TO_EP connection method */
        ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));
        pkt_event            = (uct_tcp_cm_conn_event_t*)(pkt_hdr + 1);
        *pkt_event           = event;
    }

    status = ucs_socket_send(ep->fd, pkt_buf, pkt_length);
    if (status == UCS_OK) {
        uct_tcp_cm_trace_conn_pkt(ep, UCS_LOG_LEVEL_TRACE,
                                  "%s sent to", event);
    } else {
        ucs_assert(status != UCS_ERR_NO_PROGRESS);
        status = uct_tcp_ep_handle_io_err(ep, "send", status);
        uct_tcp_cm_trace_conn_pkt(ep,
                                  (log_error && (status != UCS_ERR_CANCELED)) ?
                                  UCS_LOG_LEVEL_ERROR : UCS_LOG_LEVEL_DEBUG,
                                  "unable to send %s to", event);
    }

    return status;
}

static const void*
uct_tcp_cm_conn_match_get_address(const ucs_conn_match_elem_t *elem)
{
    const uct_tcp_ep_t *ep = ucs_container_of(elem, uct_tcp_ep_t, elem);

    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));
    return &ep->peer_addr;
}

static ucs_conn_sn_t
uct_tcp_cm_conn_match_get_conn_sn(const ucs_conn_match_elem_t *elem)
{
    const uct_tcp_ep_t *ep = ucs_container_of(elem, uct_tcp_ep_t, elem);

    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));
    return (ucs_conn_sn_t)ep->cm_id.conn_sn;
}

static const char*
uct_tcp_cm_conn_match_address_str(const ucs_conn_match_ctx_t *conn_match_ctx,
                                  const void *address, char *str,
                                  size_t max_size)
{
    return ucs_sockaddr_str((const struct sockaddr*)address,
                            str, ucs_min(max_size, UCS_SOCKADDR_STRING_LEN));
}

static void
uct_tcp_cm_conn_match_purge_cb(ucs_conn_match_ctx_t *conn_match_ctx,
                               ucs_conn_match_elem_t *elem)
{
    uct_tcp_ep_t *ep = ucs_container_of(elem, uct_tcp_ep_t, elem);

    /* EP was deleted from the connection matching context during cleanup
     * procedure, move EP to the iface's EP list to correctly destroy EP */
    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));
    ucs_assert(ep->flags & UCT_TCP_EP_FLAG_ON_MATCH_CTX);
    ep->flags &= ~UCT_TCP_EP_FLAG_ON_MATCH_CTX;
    uct_tcp_iface_add_ep(ep);

    uct_tcp_ep_destroy_internal(&ep->super.super);
}

const ucs_conn_match_ops_t uct_tcp_cm_conn_match_ops = {
    .get_address = uct_tcp_cm_conn_match_get_address,
    .get_conn_sn = uct_tcp_cm_conn_match_get_conn_sn,
    .address_str = uct_tcp_cm_conn_match_address_str,
    .purge_cb    = uct_tcp_cm_conn_match_purge_cb
};

void uct_tcp_cm_ep_set_conn_sn(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);

    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));

    ep->cm_id.conn_sn = ucs_conn_match_get_next_sn(&iface->conn_match_ctx,
                                                   &ep->peer_addr);
}

uct_tcp_ep_t *uct_tcp_cm_get_ep(uct_tcp_iface_t *iface,
                                const struct sockaddr *dest_address,
                                ucs_conn_sn_t conn_sn, uint8_t with_ctx_cap)
{
    ucs_conn_match_queue_type_t queue_type;
    ucs_conn_match_elem_t *elem;
    uct_tcp_ep_t *ep;
    int remove_from_ctx;

    ucs_assert(conn_sn < UCT_TCP_CM_CONN_SN_MAX);
    ucs_assert((with_ctx_cap == UCT_TCP_EP_FLAG_CTX_TYPE_TX) ||
               (with_ctx_cap == UCT_TCP_EP_FLAG_CTX_TYPE_RX));

    if (with_ctx_cap == UCT_TCP_EP_FLAG_CTX_TYPE_TX) {
        /* when getting CONN_REQ we search in both EXP and UNEXP queue.
         * The endpoint could be in EXP queue if it is already created from
         * API, or in UNEXP queue if the connection request message was
         * retransmitted */
        queue_type      = UCS_CONN_MATCH_QUEUE_ANY;
        remove_from_ctx = 0;
    } else {
        /* when creating new endpoint from API, search for the arrived
         * connection requests and remove from the connection matching
         * context, since the EP with RX-only capability will be destroyed
         * or re-used for the EP created through uct_ep_create() and
         * returned to the user (it will be inserted to expected queue) */
        queue_type      = UCS_CONN_MATCH_QUEUE_UNEXP;
        remove_from_ctx = 1;
    }

    elem = ucs_conn_match_get_elem(&iface->conn_match_ctx, dest_address,
                                   conn_sn, queue_type, remove_from_ctx);
    if (elem == NULL) {
        return NULL;
    }

    ep = ucs_container_of(elem, uct_tcp_ep_t, elem);
    ucs_assert(ep->flags & UCT_TCP_EP_FLAG_ON_MATCH_CTX);
    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));

    if ((queue_type == UCS_CONN_MATCH_QUEUE_UNEXP) ||
        !(ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_TX)) {
        ucs_assert(ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_RX);
    }

    if (remove_from_ctx) {
        ucs_assert((ep->flags & UCT_TCP_EP_CTX_CAPS) ==
                   UCT_TCP_EP_FLAG_CTX_TYPE_RX);
        ep->flags &= ~UCT_TCP_EP_FLAG_ON_MATCH_CTX;
        /* The EP was removed from connection matching, move it to the EP list
         * on iface to be able to destroy it from EP cleanup correctly that
         * removes the EP from the iface's EP list (an EP has to be either on
         * matching context or in iface's EP list) */
        uct_tcp_iface_add_ep(ep);
    }

    return ep;
}

void uct_tcp_cm_insert_ep(uct_tcp_iface_t *iface, uct_tcp_ep_t *ep)
{
    uint8_t ctx_caps = ep->flags & UCT_TCP_EP_CTX_CAPS;
    int ret;

    ucs_assert(ep->cm_id.conn_sn < UCT_TCP_CM_CONN_SN_MAX);
    ucs_assert((ctx_caps & UCT_TCP_EP_FLAG_CTX_TYPE_TX) ||
               (ctx_caps == UCT_TCP_EP_FLAG_CTX_TYPE_RX));
    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_ON_MATCH_CTX));
    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));

    ret = ucs_conn_match_insert(&iface->conn_match_ctx, &ep->peer_addr,
                                ep->cm_id.conn_sn, &ep->elem,
                                (ctx_caps & UCT_TCP_EP_FLAG_CTX_TYPE_TX) ?
                                UCS_CONN_MATCH_QUEUE_EXP :
                                UCS_CONN_MATCH_QUEUE_UNEXP);
    ucs_assert_always(ret == 1);

    ep->flags |= UCT_TCP_EP_FLAG_ON_MATCH_CTX;
}

void uct_tcp_cm_remove_ep(uct_tcp_iface_t *iface, uct_tcp_ep_t *ep)
{
    uint8_t ctx_caps = ep->flags & UCT_TCP_EP_CTX_CAPS;

    ucs_assert(ep->cm_id.conn_sn < UCT_TCP_CM_CONN_SN_MAX);
    ucs_assert(ctx_caps != 0);
    ucs_assert(ep->flags & UCT_TCP_EP_FLAG_ON_MATCH_CTX);
    ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP));

    ucs_conn_match_remove_elem(&iface->conn_match_ctx, &ep->elem,
                               (ctx_caps & UCT_TCP_EP_FLAG_CTX_TYPE_TX) ?
                               UCS_CONN_MATCH_QUEUE_EXP :
                               UCS_CONN_MATCH_QUEUE_UNEXP);

    ep->flags &= ~UCT_TCP_EP_FLAG_ON_MATCH_CTX;
}

int uct_tcp_cm_ep_accept_conn(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    char str_local_addr[UCS_SOCKADDR_STRING_LEN];
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    int cmp;
    ucs_status_t status;

    if (ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTED) {
        return 0;
    }

    cmp = ucs_sockaddr_cmp((const struct sockaddr*)&ep->peer_addr,
                           (const struct sockaddr*)&iface->config.ifaddr,
                           &status);
    ucs_assertv_always(status == UCS_OK, "ucs_sockaddr_cmp(%s, %s) failed",
                       ucs_sockaddr_str((const struct sockaddr*)&ep->peer_addr,
                                        str_remote_addr, UCS_SOCKADDR_STRING_LEN),
                       ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                                        str_local_addr, UCS_SOCKADDR_STRING_LEN));

    /* Accept connection from a peer if the local iface address is greater
     * than peer's one */
    return cmp < 0;
}

static unsigned
uct_tcp_cm_simult_conn_accept_remote_conn(uct_tcp_ep_t *accept_ep,
                                          uct_tcp_ep_t *connect_ep)
{
    uct_tcp_cm_conn_event_t event;
    ucs_status_t status;

    /* 1. Close the allocated socket `fd` to avoid reading any
     *    events for this socket and assign the socket `fd` returned
     *    from `accept()` to the found EP */
    uct_tcp_ep_mod_events(connect_ep, 0, connect_ep->events);
    ucs_assertv(connect_ep->events == 0,
                "Requested epoll events must be 0-ed for ep=%p", connect_ep);

    ucs_close_fd(&connect_ep->fd);
    connect_ep->fd = accept_ep->fd;

    /* 2. Migrate RX from the EP allocated during accepting connection to
     *    the found EP */
    uct_tcp_ep_move_ctx_cap(accept_ep, connect_ep, UCT_TCP_EP_FLAG_CTX_TYPE_RX);

    /* 3. The EP allocated during accepting connection has to be destroyed
     *    upon return from this function (set its socket `fd` to -1 prior
     *    to avoid closing this socket) */
    uct_tcp_ep_mod_events(accept_ep, 0, UCS_EVENT_SET_EVREAD);
    accept_ep->fd = -1;
    accept_ep     = NULL;

    /* 4. Send ACK to the peer */
    event = UCT_TCP_CM_CONN_ACK;

    /* 5. - If found EP is still connecting, tie REQ with ACK and send
     *      it to the peer using new socket fd to ensure that the peer
     *      will be able to receive the data from us
     *    - If found EP is waiting ACK, tie WAIT_REQ with ACK and send
     *      it to the peer using new socket fd to ensure that the peer
     *      will wait for REQ and after receiving the REQ, peer will
     *      be able to receive the data from us */
    if ((connect_ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTING) ||
        (connect_ep->conn_state == UCT_TCP_EP_CONN_STATE_WAITING_ACK)) {
        event |= UCT_TCP_CM_CONN_REQ;
    }

    status = uct_tcp_cm_send_event(connect_ep, event, 0);
    if (status != UCS_OK) {
        return 0;
    }
    /* 6. Now fully connected to the peer */
    uct_tcp_ep_mod_events(connect_ep, UCS_EVENT_SET_EVREAD, 0);
    uct_tcp_cm_change_conn_state(connect_ep, UCT_TCP_EP_CONN_STATE_CONNECTED);

    return 1;
}

static unsigned uct_tcp_cm_handle_simult_conn(uct_tcp_iface_t *iface,
                                              uct_tcp_ep_t *accept_ep,
                                              uct_tcp_ep_t *connect_ep)
{
    unsigned progress_count = 0;

    if (!uct_tcp_cm_ep_accept_conn(connect_ep)) {
        /* Migrate RX from the EP allocated during accepting connection to
         * the found EP. */
        uct_tcp_ep_move_ctx_cap(accept_ep, connect_ep,
                                UCT_TCP_EP_FLAG_CTX_TYPE_RX);
        uct_tcp_ep_mod_events(connect_ep, UCS_EVENT_SET_EVREAD, 0);

        /* If the EP created through API is not connected yet, don't close
         * the fd from accepted connection to avoid possible connection
         * retries from the remote peer. Save the fd to the separate field
         * for further destroying when the connection is established */
        if (connect_ep->conn_state != UCT_TCP_EP_CONN_STATE_CONNECTED) {
            uct_tcp_ep_mod_events(accept_ep, 0, UCS_EVENT_SET_EVREAD);
            ucs_assert(connect_ep->stale_fd == -1);
            connect_ep->stale_fd = accept_ep->fd;
            accept_ep->fd        = -1;
        }
    } else /* our iface address less than remote && we are not connected */ {
        /* Accept the remote connection and close the current one */
        progress_count = uct_tcp_cm_simult_conn_accept_remote_conn(accept_ep,
                                                                   connect_ep);
    }

    return progress_count;
}

static UCS_F_MAYBE_UNUSED int
uct_tcp_cm_verify_req_connected_ep(uct_tcp_ep_t *ep,
                                   const uct_tcp_cm_conn_req_pkt_t *cm_req_pkt)
{
    ucs_status_t status;

    return (ep->cm_id.conn_sn == cm_req_pkt->cm_id.conn_sn) &&
           !ucs_sockaddr_cmp((const struct sockaddr*)&ep->peer_addr,
                             (const struct sockaddr*)(cm_req_pkt + 1),
                             &status) &&
           (status == UCS_OK);
}

static unsigned
uct_tcp_cm_handle_conn_req(uct_tcp_ep_t **ep_p,
                           const uct_tcp_cm_conn_req_pkt_t *cm_req_pkt)
{
    uct_tcp_ep_t *ep        = *ep_p;
    uct_tcp_iface_t *iface  = ucs_derived_of(ep->super.super.iface,
                                             uct_tcp_iface_t);
    unsigned progress_count = 0;
    ucs_status_t status;
    uct_tcp_ep_t *peer_ep;
    int connect_to_self;

    ucs_assert(/* EP received the connection request after the TCP
                * connection was accepted */
               (ep->conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING) ||
               /* EP is already connected to this peer (conn_sn and address
                * must be the same) */
               ((ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTED) &&
                uct_tcp_cm_verify_req_connected_ep(ep, cm_req_pkt)));

    if (ep->conn_state == UCT_TCP_EP_CONN_STATE_ACCEPTING) {
        memcpy(&ep->peer_addr, cm_req_pkt + 1, iface->config.sockaddr_len);
        ep->cm_id = cm_req_pkt->cm_id;
        if (cm_req_pkt->flags & UCT_TCP_CM_CONN_REQ_PKT_FLAG_CONNECT_TO_EP) {
            ep->flags |= UCT_TCP_EP_FLAG_CONNECT_TO_EP;
        }
    }

    uct_tcp_cm_trace_conn_pkt(ep, UCS_LOG_LEVEL_TRACE,
                              "%s received from", UCT_TCP_CM_CONN_REQ);

    uct_tcp_ep_add_ctx_cap(ep, UCT_TCP_EP_FLAG_CTX_TYPE_RX);

    if (ep->conn_state == UCT_TCP_EP_CONN_STATE_CONNECTED) {
        goto send_ack;
    }

    ucs_assertv(!(ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_TX),
                "ep %p mustn't have TX cap", ep);

    connect_to_self = uct_tcp_ep_is_self(ep);
    if (connect_to_self) {
        goto accept_conn;
    }

    if (!(cm_req_pkt->flags & UCT_TCP_CM_CONN_REQ_PKT_FLAG_CONNECT_TO_EP)) {
        peer_ep = uct_tcp_cm_get_ep(iface, (struct sockaddr*)&ep->peer_addr,
                                    cm_req_pkt->cm_id.conn_sn,
                                    UCT_TCP_EP_FLAG_CTX_TYPE_TX);
        if (peer_ep != NULL) {
            progress_count = uct_tcp_cm_handle_simult_conn(iface, ep, peer_ep);
            ucs_assert(!(ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_TX));
            goto out_destroy_ep;
        }
    } else {
        ucs_assert(uct_tcp_cm_ep_accept_conn(ep));
        peer_ep = uct_tcp_ep_ptr_map_retrieve(iface, ep->cm_id.ptr_map_key);
        if (peer_ep == NULL) {
            /* Local user-exposed EP was destroyed before receiving CONN_REQ
             * from a peer, drop the connection */
            goto out_destroy_ep;
        }

        memcpy(peer_ep->peer_addr, ep->peer_addr, iface->config.sockaddr_len);
        peer_ep->conn_retries++;
        uct_tcp_ep_add_ctx_cap(peer_ep, UCT_TCP_EP_FLAG_CTX_TYPE_TX);
        uct_tcp_ep_move_ctx_cap(ep, peer_ep, UCT_TCP_EP_FLAG_CTX_TYPE_RX);
        uct_tcp_ep_replace_ep(peer_ep, ep);
        uct_tcp_cm_change_conn_state(peer_ep,
                                     UCT_TCP_EP_CONN_STATE_CONNECTED);
        goto out_destroy_ep;
    }

accept_conn:
    ucs_assert(!(cm_req_pkt->flags &
                 UCT_TCP_CM_CONN_REQ_PKT_FLAG_CONNECT_TO_EP) || connect_to_self);

    if (!connect_to_self) {
        uct_tcp_iface_remove_ep(ep);
        uct_tcp_cm_insert_ep(iface, ep);
    }

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CONN_STATE_CONNECTED);

send_ack:
    /* Just accept this connection and make it operational for RX events */
    if (!(cm_req_pkt->flags & UCT_TCP_CM_CONN_REQ_PKT_FLAG_CONNECT_TO_EP)) {
        status = uct_tcp_cm_send_event(ep, UCT_TCP_CM_CONN_ACK, 1);
        if (status != UCS_OK) {
            goto out_destroy_ep;
        }
    }

    return 1;

out_destroy_ep:
    if (!(ep->flags & UCT_TCP_EP_FLAG_CTX_TYPE_TX)) {
        uct_tcp_ep_destroy_internal(&ep->super.super);
        *ep_p = NULL;
    }
    return progress_count;
}

static void uct_tcp_cm_handle_conn_ack(uct_tcp_ep_t *ep,
                                       uct_tcp_cm_conn_event_t cm_event,
                                       uct_tcp_ep_conn_state_t new_conn_state)
{
    uct_tcp_cm_trace_conn_pkt(ep, UCS_LOG_LEVEL_TRACE,
                              "%s received from", cm_event);

    ucs_close_fd(&ep->stale_fd);
    if (ep->conn_state != new_conn_state) {
        uct_tcp_cm_change_conn_state(ep, new_conn_state);
    }
}

static void uct_tcp_cm_handle_conn_fin(uct_tcp_ep_t **ep_p)
{
    uct_tcp_ep_t *ep = *ep_p;

    if ((ep->flags & UCT_TCP_EP_CTX_CAPS) == UCT_TCP_EP_FLAG_CTX_TYPE_RX) {
        uct_tcp_ep_destroy_internal(&ep->super.super);
        *ep_p = NULL;
    } else {
        uct_tcp_ep_remove_ctx_cap(ep, UCT_TCP_EP_FLAG_CTX_TYPE_RX);
    }
}

unsigned uct_tcp_cm_handle_conn_pkt(uct_tcp_ep_t **ep_p, void *pkt, uint32_t length)
{
    uct_tcp_iface_t UCS_V_UNUSED *iface =
            ucs_derived_of((*ep_p)->super.super.iface, uct_tcp_iface_t);
    uct_tcp_cm_conn_event_t cm_event;
    uct_tcp_cm_conn_req_pkt_t *cm_req_pkt;

    ucs_assertv(length >= sizeof(cm_event), "ep=%p", *ep_p);

    cm_event = *((uct_tcp_cm_conn_event_t*)pkt);

    switch (cm_event) {
    case UCT_TCP_CM_CONN_REQ:
        /* Don't trace received CM packet here, because
         * EP doesn't contain the peer address */
        ucs_assertv(length ==
                     (sizeof(*cm_req_pkt) + iface->config.sockaddr_len),
                    "ep=%p length=%u", *ep_p, length);
        cm_req_pkt = (uct_tcp_cm_conn_req_pkt_t*)pkt;
        return uct_tcp_cm_handle_conn_req(ep_p, cm_req_pkt);
    case UCT_TCP_CM_CONN_ACK_WITH_REQ:
        uct_tcp_ep_add_ctx_cap(*ep_p, UCT_TCP_EP_FLAG_CTX_TYPE_RX);
        /* fall through */
    case UCT_TCP_CM_CONN_ACK:
        uct_tcp_cm_handle_conn_ack(*ep_p, cm_event,
                                   UCT_TCP_EP_CONN_STATE_CONNECTED);
        return 0;
    case UCT_TCP_CM_CONN_FIN:
        uct_tcp_cm_handle_conn_fin(ep_p);
        return 0;
    }

    ucs_error("tcp_ep %p: unknown CM event received %d", *ep_p, cm_event);
    return 0;
}

static void uct_tcp_cm_conn_complete(uct_tcp_ep_t *ep)
{
    ucs_status_t status;

    status = uct_tcp_cm_send_event(ep, UCT_TCP_CM_CONN_REQ, 1);
    if (status != UCS_OK) {
        /* error handling was done inside sending event operation */
        return;
    }

    if (ep->flags & UCT_TCP_EP_FLAG_CONNECT_TO_EP) {
        uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CONN_STATE_CONNECTED);
    } else {
        uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CONN_STATE_WAITING_ACK);
    }

    uct_tcp_ep_mod_events(ep, UCS_EVENT_SET_EVREAD, 0);

    ucs_assertv((ep->tx.length == 0) && (ep->tx.offset == 0) &&
                (ep->tx.buf == NULL), "ep=%p", ep);
}

unsigned uct_tcp_cm_conn_progress(void *arg)
{
    uct_tcp_ep_t *ep = (uct_tcp_ep_t*)arg;

    if (!ucs_socket_is_connected(ep->fd)) {
        ucs_error("tcp_ep %p: connection establishment for "
                  "socket fd %d was unsuccessful", ep, ep->fd);
        goto err;
    }

    uct_tcp_cm_conn_complete(ep);
    return 1;

err:
    uct_tcp_ep_set_failed(ep, UCS_ERR_ENDPOINT_TIMEOUT);
    return 0;
}

ucs_status_t uct_tcp_cm_conn_start(uct_tcp_ep_t *ep)
{
    char dest_str[UCS_SOCKADDR_STRING_LEN];
    char src_str[UCS_SOCKADDR_STRING_LEN];
    char src_str2[UCS_SOCKADDR_STRING_LEN];
    char* remote_address = NULL;
    char * token = NULL;
    int token_index = 0;
    char publicAddress[UCS_SOCKADDR_STRING_LEN];
    int publicPort = 0;
    struct sockaddr_in local_port_addr;
    socklen_t local_addr_len = sizeof(local_port_addr);
    int enable_flag = 1;
    struct sockaddr_storage connect_addr;
    int retries = 0;
    int result = 0;
    uint16_t port = 0;



    struct sockaddr* addr = NULL;

    size_t addrlen;

    int flags;
    struct timeval timeout;
    size_t addr_len;
    size_t peer_addr_len;

    fd_set set;

    int so_error;
    socklen_t len = sizeof(so_error);





    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    ucs_status_t status = UCS_OK;

    ep->conn_retries++;
    if (ep->conn_retries > iface->config.max_conn_retries) {
        ucs_warn("tcp_ep %p: reached maximum number of connection retries "
                  "(%u)", ep, iface->config.max_conn_retries);
        return UCS_ERR_TIMED_OUT;
    }

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CONN_STATE_CONNECTING);

    ucs_sockaddr_str((const struct sockaddr*)&ep->peer_addr, dest_str,
                     UCS_SOCKADDR_STRING_LEN);
    ucs_warn("uct_tcp_cm_conn_start - peer address: %s source address: %s", dest_str,
             ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr,
                              src_str, sizeof(src_str)));

    status = ucs_sockaddr_get_port((struct sockaddr*)&iface->config.ifaddr, &port);
    if (status != UCS_OK) {
      ucs_warn("unable to retrieve port for source address");
      return status;
    }


    if (iface->config.enable_nat_traversal) { //use public address from redis as peer address

      ucs_warn("nat traversal enabled - connection retries: %i", ep->conn_retries);
      remote_address = getValueFromRedis(iface->config.redis_ip_address, iface->config.redis_port, dest_str);
      while(remote_address == NULL) {
        msleep(50);
        ucs_warn("sleeping waiting for remote address from redis...");
        remote_address = getValueFromRedis(iface->config.redis_ip_address, iface->config.redis_port, dest_str);

      }

      ucs_warn("remote address returned from redis: %s", remote_address);

      token = strtok(remote_address, ":");

      while (token != NULL) {
        if (token_index == 0) {
          strcpy(publicAddress, token);
        } else if (token_index == 1){
          publicPort = atoi(token);
        }

        token = strtok(NULL, ":");
        token_index++;
      }

      ucs_warn("set public address to %s and port %i from redis", publicAddress, publicPort);

      ucs_warn("configuring to reuse socket port");
      status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_REUSEPORT,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket port");

      }

      ucs_warn("configuring to reuse socket address");
      status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_REUSEADDR,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket address");
      }

      timeout.tv_sec = 6;
      timeout.tv_usec = 0;

      ucs_warn("configuring to set connect timeout");
      status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_SNDTIMEO,
                                 &timeout,
                                 sizeof timeout);
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to set connect timeout");
      }

      if (getsockname(iface->listen_fd, (struct sockaddr*)&local_port_addr, &local_addr_len)< 0 ) {
        ucs_warn("getsockname failed");
      }



     /* local_port_addr.sin_family = AF_INET;
      local_port_addr.sin_addr.s_addr = INADDR_ANY;
      local_port_addr.sin_port = port;*/


      status = ucs_sockaddr_sizeof((struct sockaddr *)&local_port_addr, &addr_len);
      if (status != UCS_OK) {
        ucs_warn("ucs_sockaddr_sizeof failed ");
        return status;
      }


      status = ucs_socket_server_init((struct sockaddr *)&local_port_addr, addr_len,
                                      ucs_socket_max_conn(), 1, 1,
                                      &ep->fd);

      if (status != UCS_OK) {
        ucs_warn("ucs_socket_server_init failed ");
        return status;
      }

      ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr,
                       src_str2, sizeof(src_str2));

      ucs_warn("endpoint socket ip: %s", src_str2);


      /**
       * Update the peer address to the remote address returned by redis
       */
      set_sock_addr(publicAddress, &connect_addr, AF_INET, publicPort);

      addr = (struct sockaddr*)&connect_addr;

      status = ucs_sockaddr_sizeof(addr, &addrlen);
      if (status != UCS_OK) {
        return status;
      }

      if ((struct sockaddr*)&ep->peer_addr != NULL) {
        memcpy((struct sockaddr*)&ep->peer_addr, addr, addrlen);
      }

      free(remote_address);

      if(fcntl(ep->fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_warn("Setting O_NONBLOCK failed: ");
      }



      while (retries < 25) {
        ucs_warn("retrying connection - current retry: %i", retries);

        status = ucs_sockaddr_sizeof((const struct sockaddr *)&ep->peer_addr, &peer_addr_len);
        if (status != UCS_OK) {
          ucs_warn("ucs_sockaddr_sizeof failed ");
          return status;
        }

        result = connect(ep->fd, (const struct sockaddr *)&ep->peer_addr, peer_addr_len);

        if (result == 0) {
          status = UCS_OK;
          break;
        }

        FD_ZERO(&set);
        FD_SET(ep->fd, &set);
        timeout.tv_sec = 10;
        timeout.tv_usec = 0;

        result = select(ep->fd + 1, NULL, &set, NULL, &timeout);
        if (result <= 0) {
          // select() failed or connection timed out
          ucs_warn("select failed on peer socket %i", ep->fd);
        } else {
          getsockopt(ep->fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
          if (so_error == 0) {
            ucs_warn("Connected on attempt %d", retries + 1);
            status = UCS_OK;
            break;
          } else {
            ucs_warn("Connection failed: %s and continuing", strerror(so_error));
          }
        }

        close(ep->fd);

        status = ucs_socket_create(((struct sockaddr*)ep->peer_addr)->sa_family, SOCK_STREAM, &ep->fd);
        if (status != UCS_OK) {
          ucs_warn("could not create socket");
        }

        ucs_warn("configuring to reuse socket port");
        status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_REUSEPORT,
                                   &enable_flag, sizeof(int));
        if (status != UCS_OK) {
          ucs_warn("could NOT configure to reuse socket port");

        }

        ucs_warn("configuring to reuse socket address");
        status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_REUSEADDR,
                                   &enable_flag, sizeof(int));
        if (status != UCS_OK) {
          ucs_warn("could NOT configure to reuse socket address");
        }


        ucs_warn("configuring to set connect timeout");
        status = ucs_socket_setopt(ep->fd, SOL_SOCKET, SO_SNDTIMEO,
                                   &timeout,
                                   sizeof timeout);
        if (status != UCS_OK) {
          ucs_warn("could NOT configure to set connect timeout");
        }

        local_port_addr.sin_family = AF_INET;
        local_port_addr.sin_addr.s_addr = INADDR_ANY;
        local_port_addr.sin_port = port;

        /*if (bind(ep->fd, (const struct sockaddr *)&local_port_addr, sizeof(local_port_addr))) {
          ucs_warn("Binding to same port failed: ");
        }*/

        status = ucs_sockaddr_sizeof((struct sockaddr *)&local_port_addr, &addr_len);
        if (status != UCS_OK) {
          ucs_warn("ucs_sockaddr_sizeof failed ");
          return status;
        }

        status = ucs_socket_server_init((struct sockaddr *)&local_port_addr, addr_len,
                                        ucs_socket_max_conn(), 1, 0,
                                        &ep->fd);

        if (status != UCS_OK) {
          ucs_warn("ucs_socket_server_init failed ");
          return status;
        }

        ucs_warn("endpoint peer socket info(fd=%d, src_addr=%s local port=%i )", ep->fd,
                 ucs_socket_getname_str(ep->fd, src_str2, UCS_SOCKADDR_STRING_LEN), port);

        if(fcntl(ep->fd, F_SETFL, O_NONBLOCK) != 0) {
          ucs_warn("Setting O_NONBLOCK failed: ");
        }

        retries++;
      }

      flags = fcntl(ep->fd,  F_GETFL, 0);
      flags &= ~(O_NONBLOCK);
      fcntl(ep->fd, F_SETFL, flags);

      /*if (UCS_STATUS_IS_ERR(status)) {
        return status;
      } else if (status == UCS_INPROGRESS) {
        ucs_assert(iface->config.conn_nb);
        uct_tcp_ep_mod_events(ep, UCS_EVENT_SET_EVWRITE, 0);
        return UCS_OK;
      }*/

      ucs_assert(status == UCS_OK);

      if (!iface->config.conn_nb) {
        status = ucs_sys_fcntl_modfl(ep->fd, O_NONBLOCK, 0);
        if (status != UCS_OK) {
          return status;
        }
      }

      uct_tcp_cm_conn_complete(ep);
      return UCS_OK;


    } else {

      status =
          ucs_socket_connect(ep->fd, (const struct sockaddr *)&ep->peer_addr);
      if (UCS_STATUS_IS_ERR(status)) {
        return status;
      } else if (status == UCS_INPROGRESS) {
        ucs_assert(iface->config.conn_nb);
        uct_tcp_ep_mod_events(ep, UCS_EVENT_SET_EVWRITE, 0);
        return UCS_OK;
      }

      ucs_assert(status == UCS_OK);

      if (!iface->config.conn_nb) {
        status = ucs_sys_fcntl_modfl(ep->fd, O_NONBLOCK, 0);
        if (status != UCS_OK) {
          return status;
        }
      }

      uct_tcp_cm_conn_complete(ep);
      return UCS_OK;
    }
}

/* This function is called from async thread */
ucs_status_t uct_tcp_cm_handle_incoming_conn(uct_tcp_iface_t *iface,
                                             const struct sockaddr *peer_addr,
                                             int fd)
{
    char str_local_addr[UCS_SOCKADDR_STRING_LEN];
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    ucs_status_t status;
    uct_tcp_ep_t *ep;

    if (!ucs_socket_is_connected(fd)) {
        ucs_warn("tcp_iface %p: connection establishment for socket fd %d "
                 "from %s to %s was unsuccessful", iface, fd,
                 ucs_sockaddr_str(peer_addr, str_remote_addr,
                                  UCS_SOCKADDR_STRING_LEN),
                 ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                                  str_local_addr, UCS_SOCKADDR_STRING_LEN));
        return UCS_ERR_UNREACHABLE;
    }

    /* set non-blocking flag, since this is a fd from accept(), i.e.
     * connection was already established */
    status = uct_tcp_iface_set_sockopt(iface, fd, 1);
    if (status != UCS_OK) {
        return status;
    }

    status = uct_tcp_ep_init(iface, fd, NULL, &ep);
    if (status != UCS_OK) {
        return status;
    }

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CONN_STATE_RECV_MAGIC_NUMBER);
    uct_tcp_ep_mod_events(ep, UCS_EVENT_SET_EVREAD, 0);

    ucs_debug("tcp_iface %p: accepted connection from "
              "%s on %s to tcp_ep %p (fd %d)", iface,
              ucs_sockaddr_str(peer_addr, str_remote_addr,
                               UCS_SOCKADDR_STRING_LEN),
              ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                               str_local_addr, UCS_SOCKADDR_STRING_LEN),
              ep, fd);
    return UCS_OK;
}
