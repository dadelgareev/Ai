# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

import fileexchange_pb2 as fileexchange__pb2
import processingdatabase_pb2 as processingdatabase__pb2

GRPC_GENERATED_VERSION = '1.68.1'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in processingdatabase_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class ProcessingDatabaseStub(object):
    """The processing database service, shared for scrapper, preprocessor and processor units
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Insert = channel.unary_unary(
                '/ProcessingDatabase.ProcessingDatabase/Insert',
                request_serializer=processingdatabase__pb2.InsertRequest.SerializeToString,
                response_deserializer=processingdatabase__pb2.InsertResult.FromString,
                _registered_method=True)
        self.ImportCSV = channel.stream_unary(
                '/ProcessingDatabase.ProcessingDatabase/ImportCSV',
                request_serializer=fileexchange__pb2.FileData.SerializeToString,
                response_deserializer=fileexchange__pb2.UploadStatus.FromString,
                _registered_method=True)
        self.GetByStatus = channel.unary_unary(
                '/ProcessingDatabase.ProcessingDatabase/GetByStatus',
                request_serializer=processingdatabase__pb2.GetByStatusRequest.SerializeToString,
                response_deserializer=processingdatabase__pb2.GetByStatusResult.FromString,
                _registered_method=True)
        self.UpdateByID = channel.unary_unary(
                '/ProcessingDatabase.ProcessingDatabase/UpdateByID',
                request_serializer=processingdatabase__pb2.UpdateByIDRequest.SerializeToString,
                response_deserializer=processingdatabase__pb2.UpdateByIDResult.FromString,
                _registered_method=True)


class ProcessingDatabaseServicer(object):
    """The processing database service, shared for scrapper, preprocessor and processor units
    """

    def Insert(self, request, context):
        """Inserts data into a table, allowed only for scrapper
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ImportCSV(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetByStatus(self, request, context):
        """Getting data by status, allowed only for preprocessor and processor
        For preprocessor allowed status is only NEW, for processor allowed NEW and PREPROCESSED (depends on processor posibilities) status
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateByID(self, request, context):
        """Updating data after preprocessing or processing, status will be updated automatically
        all checkings will be performed by the server
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ProcessingDatabaseServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Insert': grpc.unary_unary_rpc_method_handler(
                    servicer.Insert,
                    request_deserializer=processingdatabase__pb2.InsertRequest.FromString,
                    response_serializer=processingdatabase__pb2.InsertResult.SerializeToString,
            ),
            'ImportCSV': grpc.stream_unary_rpc_method_handler(
                    servicer.ImportCSV,
                    request_deserializer=fileexchange__pb2.FileData.FromString,
                    response_serializer=fileexchange__pb2.UploadStatus.SerializeToString,
            ),
            'GetByStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.GetByStatus,
                    request_deserializer=processingdatabase__pb2.GetByStatusRequest.FromString,
                    response_serializer=processingdatabase__pb2.GetByStatusResult.SerializeToString,
            ),
            'UpdateByID': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateByID,
                    request_deserializer=processingdatabase__pb2.UpdateByIDRequest.FromString,
                    response_serializer=processingdatabase__pb2.UpdateByIDResult.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'ProcessingDatabase.ProcessingDatabase', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('ProcessingDatabase.ProcessingDatabase', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class ProcessingDatabase(object):
    """The processing database service, shared for scrapper, preprocessor and processor units
    """

    @staticmethod
    def Insert(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/ProcessingDatabase.ProcessingDatabase/Insert',
            processingdatabase__pb2.InsertRequest.SerializeToString,
            processingdatabase__pb2.InsertResult.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def ImportCSV(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(
            request_iterator,
            target,
            '/ProcessingDatabase.ProcessingDatabase/ImportCSV',
            fileexchange__pb2.FileData.SerializeToString,
            fileexchange__pb2.UploadStatus.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetByStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/ProcessingDatabase.ProcessingDatabase/GetByStatus',
            processingdatabase__pb2.GetByStatusRequest.SerializeToString,
            processingdatabase__pb2.GetByStatusResult.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def UpdateByID(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/ProcessingDatabase.ProcessingDatabase/UpdateByID',
            processingdatabase__pb2.UpdateByIDRequest.SerializeToString,
            processingdatabase__pb2.UpdateByIDResult.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)


class ImageEmbeddingServiceStub(object):
    """=== Сервис для взаимодействия с нейронной сетью ===
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ProcessImage = channel.unary_unary(
                '/ProcessingDatabase.ImageEmbeddingService/ProcessImage',
                request_serializer=processingdatabase__pb2.EmbeddingRequest.SerializeToString,
                response_deserializer=processingdatabase__pb2.EmbeddingResponse.FromString,
                _registered_method=True)


class ImageEmbeddingServiceServicer(object):
    """=== Сервис для взаимодействия с нейронной сетью ===
    """

    def ProcessImage(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ImageEmbeddingServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ProcessImage': grpc.unary_unary_rpc_method_handler(
                    servicer.ProcessImage,
                    request_deserializer=processingdatabase__pb2.EmbeddingRequest.FromString,
                    response_serializer=processingdatabase__pb2.EmbeddingResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'ProcessingDatabase.ImageEmbeddingService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('ProcessingDatabase.ImageEmbeddingService', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class ImageEmbeddingService(object):
    """=== Сервис для взаимодействия с нейронной сетью ===
    """

    @staticmethod
    def ProcessImage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/ProcessingDatabase.ImageEmbeddingService/ProcessImage',
            processingdatabase__pb2.EmbeddingRequest.SerializeToString,
            processingdatabase__pb2.EmbeddingResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)