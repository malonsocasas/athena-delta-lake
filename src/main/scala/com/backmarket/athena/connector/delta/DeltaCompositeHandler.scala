package com.backmarket.athena.connector.delta

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler

class DeltaCompositeHandler extends CompositeHandler(new DeltaMetadataHandler(), new DeltaRecordHandler())
