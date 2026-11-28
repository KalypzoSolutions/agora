package de.kalypzo.agora.service;

import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.*;
import io.grpc.stub.StreamObserver;

/**
 * The single gRPC service implementation. Every RPC answers UNIMPLEMENTED until a handler is registered; later
 * cards plug in their endpoints with the {@code on<Rpc>} methods (each RPC has its own slot, so cards do not
 * touch each other). Register handlers before the server starts.
 */
public final class AgoraService extends AgoraGrpc.AgoraImplBase {

    private volatile ConnectHandler connect;
    private volatile RpcHandler<ClaimPresenceRequest, ClaimPresenceResponse> claimPresence;
    private volatile RpcHandler<LocateRequest, LocateResponse> locate;
    private volatile RpcHandler<TeleportRequest, TeleportResponse> teleport;
    private volatile RpcHandler<LoadPlayerRequest, LoadPlayerResponse> loadPlayer;
    private volatile RpcHandler<LoadRequest, LoadResponse> load;
    private volatile RpcHandler<QueryRequest, QueryResponse> query;
    private volatile RpcHandler<CountRequest, CountResponse> count;
    private volatile RpcHandler<VersionsRequest, VersionsResponse> versions;
    private volatile RpcHandler<ApplyRequest, ApplyResponse> apply;
    private volatile RpcHandler<CreateRequest, CreateResponse> create;
    private volatile RpcHandler<DeleteRequest, DeleteResponse> delete;
    private volatile RpcHandler<ExpireRequest, ExpireResponse> expire;
    private volatile RpcHandler<TransferRequest, TransferResponse> transfer;
    private volatile RpcHandler<DeclareIndexRequest, DeclareIndexResponse> declareIndex;
    private volatile RpcHandler<AuditBundle, AppendAuditResponse> appendAudit;
    private volatile RpcHandler<TimelineRequest, TimelineResponse> timeline;

    public AgoraService onConnect(ConnectHandler handler) {
        this.connect = handler;
        return this;
    }

    @Override
    public StreamObserver<NodeMessage> connect(StreamObserver<ServiceMessage> out) {
        ConnectHandler h = connect;
        return h != null ? h.connect(out) : super.connect(out);
    }

    public AgoraService onClaimPresence(RpcHandler<ClaimPresenceRequest, ClaimPresenceResponse> handler) {
        this.claimPresence = handler;
        return this;
    }

    @Override
    public void claimPresence(ClaimPresenceRequest request, StreamObserver<ClaimPresenceResponse> out) {
        RpcHandler<ClaimPresenceRequest, ClaimPresenceResponse> h = claimPresence;
        if (h != null) h.handle(request, out);
        else super.claimPresence(request, out);
    }

    public AgoraService onLocate(RpcHandler<LocateRequest, LocateResponse> handler) {
        this.locate = handler;
        return this;
    }

    @Override
    public void locate(LocateRequest request, StreamObserver<LocateResponse> out) {
        RpcHandler<LocateRequest, LocateResponse> h = locate;
        if (h != null) h.handle(request, out);
        else super.locate(request, out);
    }

    public AgoraService onTeleport(RpcHandler<TeleportRequest, TeleportResponse> handler) {
        this.teleport = handler;
        return this;
    }

    @Override
    public void teleport(TeleportRequest request, StreamObserver<TeleportResponse> out) {
        RpcHandler<TeleportRequest, TeleportResponse> h = teleport;
        if (h != null) h.handle(request, out);
        else super.teleport(request, out);
    }

    public AgoraService onLoadPlayer(RpcHandler<LoadPlayerRequest, LoadPlayerResponse> handler) {
        this.loadPlayer = handler;
        return this;
    }

    @Override
    public void loadPlayer(LoadPlayerRequest request, StreamObserver<LoadPlayerResponse> out) {
        RpcHandler<LoadPlayerRequest, LoadPlayerResponse> h = loadPlayer;
        if (h != null) h.handle(request, out);
        else super.loadPlayer(request, out);
    }

    public AgoraService onLoad(RpcHandler<LoadRequest, LoadResponse> handler) {
        this.load = handler;
        return this;
    }

    @Override
    public void load(LoadRequest request, StreamObserver<LoadResponse> out) {
        RpcHandler<LoadRequest, LoadResponse> h = load;
        if (h != null) h.handle(request, out);
        else super.load(request, out);
    }

    public AgoraService onQuery(RpcHandler<QueryRequest, QueryResponse> handler) {
        this.query = handler;
        return this;
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> out) {
        RpcHandler<QueryRequest, QueryResponse> h = query;
        if (h != null) h.handle(request, out);
        else super.query(request, out);
    }

    public AgoraService onCount(RpcHandler<CountRequest, CountResponse> handler) {
        this.count = handler;
        return this;
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> out) {
        RpcHandler<CountRequest, CountResponse> h = count;
        if (h != null) h.handle(request, out);
        else super.count(request, out);
    }

    public AgoraService onVersions(RpcHandler<VersionsRequest, VersionsResponse> handler) {
        this.versions = handler;
        return this;
    }

    @Override
    public void versions(VersionsRequest request, StreamObserver<VersionsResponse> out) {
        RpcHandler<VersionsRequest, VersionsResponse> h = versions;
        if (h != null) h.handle(request, out);
        else super.versions(request, out);
    }

    public AgoraService onApply(RpcHandler<ApplyRequest, ApplyResponse> handler) {
        this.apply = handler;
        return this;
    }

    @Override
    public void apply(ApplyRequest request, StreamObserver<ApplyResponse> out) {
        RpcHandler<ApplyRequest, ApplyResponse> h = apply;
        if (h != null) h.handle(request, out);
        else super.apply(request, out);
    }

    public AgoraService onCreate(RpcHandler<CreateRequest, CreateResponse> handler) {
        this.create = handler;
        return this;
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> out) {
        RpcHandler<CreateRequest, CreateResponse> h = create;
        if (h != null) h.handle(request, out);
        else super.create(request, out);
    }

    public AgoraService onDelete(RpcHandler<DeleteRequest, DeleteResponse> handler) {
        this.delete = handler;
        return this;
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> out) {
        RpcHandler<DeleteRequest, DeleteResponse> h = delete;
        if (h != null) h.handle(request, out);
        else super.delete(request, out);
    }

    public AgoraService onExpire(RpcHandler<ExpireRequest, ExpireResponse> handler) {
        this.expire = handler;
        return this;
    }

    @Override
    public void expire(ExpireRequest request, StreamObserver<ExpireResponse> out) {
        RpcHandler<ExpireRequest, ExpireResponse> h = expire;
        if (h != null) h.handle(request, out);
        else super.expire(request, out);
    }

    public AgoraService onTransfer(RpcHandler<TransferRequest, TransferResponse> handler) {
        this.transfer = handler;
        return this;
    }

    @Override
    public void transfer(TransferRequest request, StreamObserver<TransferResponse> out) {
        RpcHandler<TransferRequest, TransferResponse> h = transfer;
        if (h != null) h.handle(request, out);
        else super.transfer(request, out);
    }

    public AgoraService onDeclareIndex(RpcHandler<DeclareIndexRequest, DeclareIndexResponse> handler) {
        this.declareIndex = handler;
        return this;
    }

    @Override
    public void declareIndex(DeclareIndexRequest request, StreamObserver<DeclareIndexResponse> out) {
        RpcHandler<DeclareIndexRequest, DeclareIndexResponse> h = declareIndex;
        if (h != null) h.handle(request, out);
        else super.declareIndex(request, out);
    }

    public AgoraService onAppendAudit(RpcHandler<AuditBundle, AppendAuditResponse> handler) {
        this.appendAudit = handler;
        return this;
    }

    @Override
    public void appendAudit(AuditBundle request, StreamObserver<AppendAuditResponse> out) {
        RpcHandler<AuditBundle, AppendAuditResponse> h = appendAudit;
        if (h != null) h.handle(request, out);
        else super.appendAudit(request, out);
    }

    public AgoraService onTimeline(RpcHandler<TimelineRequest, TimelineResponse> handler) {
        this.timeline = handler;
        return this;
    }

    @Override
    public void timeline(TimelineRequest request, StreamObserver<TimelineResponse> out) {
        RpcHandler<TimelineRequest, TimelineResponse> h = timeline;
        if (h != null) h.handle(request, out);
        else super.timeline(request, out);
    }
}
