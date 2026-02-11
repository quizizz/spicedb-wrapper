"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AuthZed = exports.RelationshipUpdateOperation = exports.ClientSecurity = void 0;
const authzed_node_1 = require("@authzed/authzed-node");
const grpc = __importStar(require("@grpc/grpc-js"));
const logger_1 = require("../logger");
const ClientSecurity = authzed_node_1.v1.ClientSecurity;
exports.ClientSecurity = ClientSecurity;
const RelationshipUpdateOperation = authzed_node_1.v1.RelationshipUpdate_Operation;
exports.RelationshipUpdateOperation = RelationshipUpdateOperation;
class AuthZed {
    _client;
    logger;
    constructor(params, { logger, }) {
        this._client = authzed_node_1.v1.NewClient(params.token, params.host, params.security ?? authzed_node_1.v1.ClientSecurity.INSECURE_PLAINTEXT_CREDENTIALS, undefined, params.grpcClientOptions || {});
        this.logger = logger || new logger_1.ConsoleLogger();
    }
    _getConsistencyParams(request) {
        if (!request.consistency ||
            request.consistency.type === 'minimum-latency') {
            return {
                requirement: {
                    minimizeLatency: true,
                    oneofKind: 'minimizeLatency',
                },
            };
        }
        let consistency = null;
        switch (request.consistency.type) {
            case 'at-least-as-fresh':
                consistency = {
                    requirement: {
                        atLeastAsFresh: request.consistency.zedToken,
                        oneofKind: 'atLeastAsFresh',
                    },
                };
                break;
            case 'fully-consistent':
                consistency = {
                    requirement: {
                        fullyConsistent: true,
                        oneofKind: 'fullyConsistent',
                    },
                };
                break;
            default:
                consistency = {
                    requirement: {
                        minimizeLatency: true,
                        oneofKind: 'minimizeLatency',
                    },
                };
        }
        return consistency;
    }
    _handleDataStream(stream) {
        return new Promise((resolve, reject) => {
            const accumulator = [];
            stream.on('data', (chunk) => {
                accumulator.push(chunk);
            });
            stream.on('end', () => {
                resolve(accumulator);
            });
            stream.on('close', () => {
                resolve(accumulator);
            });
            stream.on('error', (err) => {
                reject(err);
            });
        });
    }
    getClient() {
        return this._client;
    }
    writeSchema(schema) {
        const writeSchemaRequest = authzed_node_1.v1.WriteSchemaRequest.create({
            schema,
        });
        this.logger.infoj({
            msg: 'Writing schema to SpiceDB',
            schema,
        });
        return new Promise((resolve, reject) => {
            this._client.writeSchema(writeSchemaRequest, {}, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(true);
            });
        });
    }
    readSchema() {
        const readSchemaRequest = authzed_node_1.v1.ReadSchemaRequest.create();
        return new Promise((resolve, reject) => {
            this._client.readSchema(readSchemaRequest, {}, (err, resp) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(resp.schemaText);
            });
        });
    }
    updateRelations(params) {
        const updates = params.updates.map((update) => {
            const subject = authzed_node_1.v1.SubjectReference.create({
                object: {
                    objectId: update.accessor.id,
                    objectType: update.accessor.type,
                },
                optionalRelation: update.accessor.subRelation,
            });
            const object = authzed_node_1.v1.ObjectReference.create({
                objectId: update.resource.id,
                objectType: update.resource.type,
            });
            return {
                relationship: {
                    relation: update.relation,
                    subject,
                    resource: object,
                },
                operation: update.operation,
            };
        });
        this.logger.debugj({
            msg: 'Updating relations in SpiceDB',
            updates,
        });
        const updateRelationsRequest = authzed_node_1.v1.WriteRelationshipsRequest.create({
            updates,
        });
        return new Promise((resolve, reject) => {
            this._client.writeRelationships(updateRelationsRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {}, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(res.writtenAt);
            });
        });
    }
    deleteRelations(params) {
        const { resource, subject, relation } = params;
        const deleteRelationshipsRequest = authzed_node_1.v1.DeleteRelationshipsRequest.create({
            relationshipFilter: {
                resourceType: resource.type,
                optionalRelation: relation,
                optionalResourceId: resource.id,
                optionalSubjectFilter: subject
                    ? authzed_node_1.v1.SubjectFilter.create({
                        optionalRelation: authzed_node_1.v1.SubjectFilter_RelationFilter.create({
                            relation: subject.subRelation,
                        }),
                        optionalSubjectId: subject.id,
                        subjectType: subject.type,
                    })
                    : undefined,
            },
        });
        this.logger.debugj({
            msg: 'Deleting relations in SpiceDB',
            data: deleteRelationshipsRequest,
        });
        return new Promise((resolve, reject) => {
            this._client.deleteRelationships(deleteRelationshipsRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {}, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(res.deletedAt);
            });
        });
    }
    addRelations({ relations = [], grpcOptions = {}, grpcMetadata = null, }) {
        const updates = relations.map((relation) => {
            const subject = authzed_node_1.v1.SubjectReference.create({
                object: {
                    objectId: relation.subject.id,
                    objectType: relation.subject.type,
                },
                optionalRelation: relation.subject.subRelation,
            });
            const object = authzed_node_1.v1.ObjectReference.create({
                objectId: relation.resource.id,
                objectType: relation.resource.type,
            });
            return {
                relationship: {
                    relation: relation.relation,
                    subject,
                    resource: object,
                },
                operation: authzed_node_1.v1.RelationshipUpdate_Operation.TOUCH,
            };
        });
        this.logger.debugj({
            msg: 'Creating relations in SpiceDB',
            updates,
        });
        const addRelationRequest = authzed_node_1.v1.WriteRelationshipsRequest.create({
            updates,
        });
        return new Promise((resolve, reject) => {
            this._client.writeRelationships(addRelationRequest, grpcMetadata || new grpc.Metadata(), grpcOptions || {}, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(res.writtenAt);
            });
        });
    }
    async readRelationships(params) {
        const subjectFilter = {};
        if (params.subject?.id) {
            subjectFilter.optionalSubjectId = params.subject.id;
        }
        if (params.subject?.type) {
            subjectFilter.subjectType = params.subject.type;
        }
        if (params.subject?.subRelation) {
            subjectFilter.optionalRelation = {
                relation: params.subject.subRelation,
            };
        }
        const request = authzed_node_1.v1.ReadRelationshipsRequest.create({
            consistency: this._getConsistencyParams(params),
            relationshipFilter: {
                optionalRelation: params.relation,
                optionalResourceId: params.resource.id,
                resourceType: params.resource.type,
                optionalSubjectFilter: params.subject ? subjectFilter : undefined,
            },
        });
        this.logger.debugj({
            msg: 'Reading relationships',
            params: request.relationshipFilter.optionalSubjectFilter,
        });
        const stream = this._client.readRelationships(request, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {});
        const relationships = await this._handleDataStream(stream);
        const result = relationships.map((result) => ({
            zedToken: result.readAt,
            resource: {
                type: result.relationship.resource.objectType,
                id: result.relationship.resource.objectId,
            },
            subject: {
                subRelation: result.relationship.subject.optionalRelation,
                id: result.relationship.subject.object.objectId,
                type: result.relationship.subject.object.objectType,
            },
            relation: result.relationship.relation,
        }));
        return result;
    }
    checkPermission(params) {
        const resource = authzed_node_1.v1.ObjectReference.create({
            objectId: params.resource.id,
            objectType: params.resource.type,
        });
        const subject = authzed_node_1.v1.SubjectReference.create({
            object: {
                objectId: params.accessor.id,
                objectType: params.accessor.type,
            },
            optionalRelation: params.accessor.subRelation,
        });
        const checkPermParams = {
            permission: params.permission,
            resource,
            subject,
            consistency: this._getConsistencyParams(params),
        };
        this.logger.debugj({
            msg: 'Checking for permissions',
            params: checkPermParams,
        });
        const checkPermissionRequest = authzed_node_1.v1.CheckPermissionRequest.create(checkPermParams);
        return new Promise((resolve, reject) => {
            this._client.checkPermission(checkPermissionRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {}, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                const hasPermissions = res.permissionship ===
                    authzed_node_1.v1.CheckPermissionResponse_Permissionship.HAS_PERMISSION;
                resolve(hasPermissions);
            });
        });
    }
    async listResourcesAccessorCanAccess(params) {
        const lookupRequestParams = {
            resourceObjectType: params.resourceType,
            subject: authzed_node_1.v1.SubjectReference.create({
                object: {
                    objectId: params.accessor.id,
                    objectType: params.accessor.type,
                },
                optionalRelation: params.accessor.subRelation ?? undefined,
            }),
            permission: params.permission,
            consistency: this._getConsistencyParams(params),
        };
        const lookupResourcesRequest = authzed_node_1.v1.LookupResourcesRequest.create(lookupRequestParams);
        this.logger.debugj({
            msg: 'Listing resources for accessor',
            lookupResourcesRequest,
        });
        const stream = this._client.lookupResources(lookupResourcesRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {});
        const resources = await this._handleDataStream(stream);
        const response = resources.map((resource) => ({
            resourceId: resource.resourceObjectId,
            zedToken: resource.lookedUpAt.token,
        }));
        return response;
    }
    async listAccessorsForResource(params) {
        const lookupSubjectsRequest = authzed_node_1.v1.LookupSubjectsRequest.create({
            subjectObjectType: params.subjectType,
            resource: authzed_node_1.v1.ObjectReference.create({
                objectId: params.resource.id,
                objectType: params.resource.type,
            }),
            permission: params.permission,
            optionalSubjectRelation: params.subjectRelation ?? undefined,
            consistency: this._getConsistencyParams(params),
        });
        const stream = this._client.lookupSubjects(lookupSubjectsRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {});
        const response = await this._handleDataStream(stream);
        const accessors = response.map((response) => ({
            accessorId: response.subjectObjectId,
            zedToken: response.lookedUpAt.token,
        }));
        return accessors;
    }
    registerWatchEventListener(params) {
        const watchRequest = authzed_node_1.v1.WatchRequest.create({
            optionalStartCursor: params.watchFromToken,
            optionalObjectTypes: params.objectTypes ?? [],
        });
        const watchStream = this._client.watch(watchRequest, params.grpcMetadata || new grpc.Metadata(), params.grpcOptions || {});
        this.logger.debugj({
            msg: 'Registered watch listener',
            params,
        });
        const emitter = params.emitter;
        watchStream.on('data', (watchEvent) => {
            this.logger.debugj({
                msg: 'Got watch data',
                watchEvent,
            });
            emitter.emit('data', {
                eventName: 'RelationshipUpdate',
                data: {
                    zedToken: watchEvent.changesThrough,
                    updates: watchEvent.updates,
                },
            });
        });
        watchStream.on('close', () => emitter.emit('close'));
        watchStream.on('end', () => emitter.emit('end'));
        watchStream.on('error', (err) => emitter.emit('error', err));
    }
}
exports.AuthZed = AuthZed;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXV0aHplZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uL3NyYy9saWIvYXV0aHplZC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUNBLHdEQUEyQztBQUUzQyxvREFBc0M7QUFFdEMsc0NBQW1EO0FBR25ELE1BQU0sY0FBYyxHQUFHLGlCQUFFLENBQUMsY0FBYyxDQUFDO0FBa0J2Qyx3Q0FBYztBQWpCaEIsTUFBTSwyQkFBMkIsR0FBRyxpQkFBRSxDQUFDLDRCQUE0QixDQUFDO0FBb0JsRSxrRUFBMkI7QUF3TDdCLE1BQWEsT0FBTztJQUNWLE9BQU8sQ0FBa0M7SUFDekMsTUFBTSxDQUFVO0lBRXhCLFlBQ0UsTUFBMkIsRUFDM0IsRUFDRSxNQUFNLEdBR1A7UUFFRCxJQUFJLENBQUMsT0FBTyxHQUFHLGlCQUFFLENBQUMsU0FBUyxDQUN6QixNQUFNLENBQUMsS0FBSyxFQUNaLE1BQU0sQ0FBQyxJQUFJLEVBQ1gsTUFBTSxDQUFDLFFBQVEsSUFBSSxpQkFBRSxDQUFDLGNBQWMsQ0FBQyw4QkFBOEIsRUFDbkUsU0FBUyxFQUNULE1BQU0sQ0FBQyxpQkFBaUIsSUFBSSxFQUFFLENBQy9CLENBQUM7UUFDRixJQUFJLENBQUMsTUFBTSxHQUFHLE1BQU0sSUFBSSxJQUFJLHNCQUFhLEVBQUUsQ0FBQztJQUM5QyxDQUFDO0lBRUQscUJBQXFCLENBQ25CLE9BQVU7UUFFVixJQUNFLENBQUMsT0FBTyxDQUFDLFdBQVc7WUFDcEIsT0FBTyxDQUFDLFdBQVcsQ0FBQyxJQUFJLEtBQUssaUJBQWlCLEVBQzlDO1lBQ0EsT0FBTztnQkFDTCxXQUFXLEVBQUU7b0JBQ1gsZUFBZSxFQUFFLElBQUk7b0JBQ3JCLFNBQVMsRUFBRSxpQkFBaUI7aUJBQzdCO2FBQ0YsQ0FBQztTQUNIO1FBRUQsSUFBSSxXQUFXLEdBQW1DLElBQUksQ0FBQztRQUV2RCxRQUFRLE9BQU8sQ0FBQyxXQUFXLENBQUMsSUFBSSxFQUFF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