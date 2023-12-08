import { ApiDefinition, DataField, LayerApisList, VoidField, unmarshal } from "pepelaz";
import { IQueryInterface } from "./query-interfaces";
import { ITypedFacade, typedFacade } from "./typed-facade";

export type ReportedEvent = {
    body: string
}

export type ApiAsyncImplementation<T extends ApiDefinition> = {
    [P in keyof T]: T[P]["arg"] extends DataField<infer S> ?
    T[P]["ret"] extends DataField<infer R> ?
    (props: ImplementationProps, arg: S) => Promise<R>
    : never : never;
};
export type LayerApisImplementations<T extends LayerApisList> = {
    [K in keyof T]?: ApiAsyncImplementation<T[K]>
}

export const CONNECTED = "@connected";
export type InputProps = {
    db: IQueryInterface
}
export type ImplementationProps = {
    db: ITypedFacade
}
export class IntegrationHandler<T extends LayerApisList = any> {
    constructor(private apisList: T, private implementations: LayerApisImplementations<T>) { }

    handle = async <R extends keyof T>(
        props: InputProps,
        apiKey: R,
        func: keyof T[R],
        event: ReportedEvent,
        testConnection: boolean) => {
        const caller = this.implementations[apiKey]?.[func];
        if (!caller) throw new Error("Function not implemented");
        if (testConnection) return Promise.resolve(CONNECTED);
        (BigInt.prototype as any).toJSON = function () { return this.toString(); }
        const template = this.apisList[apiKey][func];
        const argument =
            template.arg instanceof VoidField ?
                void {} :
                unmarshal(template.arg, JSON.parse(event.body));
        return await caller({
            db: typedFacade(props.db)
        }, argument);
    }
}