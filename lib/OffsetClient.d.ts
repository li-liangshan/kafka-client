export declare class OffsetClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    offsetInstance: any;
    autoReconnectCount: number;
    kafkaClient: any;
    constructor(options: any);
    connect(): Promise<void>;
    onConnected(): Promise<any>;
    onReconnecting(): Promise<void>;
    close(): Promise<boolean>;
    onClosed(): Promise<boolean>;
    fetch(payloads: any): Promise<{}>;
    commit(groupId: string | number, payloads: any): Promise<{}>;
    fetchCommits(groupId: string | number, payloads: any): Promise<{}>;
    fetchLatestOffsets(topics: string | string[]): Promise<{}>;
    fetchEarliestOffsets(topics: string | string[]): Promise<{}>;
    private parseTopics(topics);
    private checkOrConnectClient();
}
