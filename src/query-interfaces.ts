export interface IQueryInterface {
    query(request: string, queryObject?: any): Promise<{ records: any[] }>;
}
