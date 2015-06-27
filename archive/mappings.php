<?php

$body = array(
	'mappings' => array(
		self::PLACES_TYPE => array(
			'_timestamp' => array(
				'enabled' => true
			),
			'properties' => array(
                'type' => array(
                    'type' => 'string'
                ),
                'features' => array(
                    'properties' => array(
                        'type' => array(
                            'type' => 'string'
                        ),
                        'id' => array(
                            'type' => 'long'
                        ),
                        'bbox' => array(
                            'type' => 'double'
                        ),
                        'geometry' => array(
                            'properties' => array(
                                'coordinates' => array(
                                    'type' => 'geo_point'
                                ),
                                'type' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'properties' => array(
                            'properties' => array(
                                'id_supersedes' => array(
                                    'type' => 'long'
                                ),
                                'id_superseded_by' => array(
                                    'type' => 'long'
                                ),
                                'envelope' => array(
                                    'type' => 'envelope'
                                ),
                                'history' => array(
                                    'properties' => array(
                                        'source' => array(
                                            'type' => 'string'
                                        ),
                                        'timestamp' => array(
                                            'type' => 'long'
                                        )
                                    )
                                ),
                                'iso' => array(
                                    'type' => 'string'
                                ),
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                ),
                                'parent' => array(
                                    'type' => 'long'
                                ),
                                'id_adjacent' => array(
                                    'type' => 'long'
                                ),
                                'alias_Q' => array(
                                    'name' => array(
                                        'type' => 'string'
                                    ),
                                    'lang' => array(
                                        'type' => 'string'
                                    )
                                ),
                                'alias_V' => array(
                                    'name' => array(
                                        'type' => 'string'
                                    ),
                                    'lang' => array(
                                        'type' => 'string'
                                    )
                                ),
                                'alias_A' => array(
                                    'name' => array(
                                        'type' => 'string'
                                    ),
                                    'lang' => array(
                                        'type' => 'string'
                                    )
                                ),
                                'alias_S' => array(
                                    'name' => array(
                                        'type' => 'string'
                                    ),
                                    'lang' => array(
                                        'type' => 'string'
                                    )
                                )
                            )
                        )
                    )
                ),


				'centroid' => array(
					'type' => 'geo_point'
				),
				'bbox' => array(
					'type' => 'object',
					'properties' => array(
						'ne' => array(
							'type' => 'geo_point'
						),
						'sw' => array(
							'type' => 'geo_point'
						)
					)
				),
				'parent' => array(
					'type' => 'long'
				),
				'woeid' => array(
					'type' => 'long'
				),
				'woeid_adjacent' => array(
					'type' => 'long'
				)
			)
		),
		self::ADMINS_TYPE => array(
			'_timestamp' => array(
				'enabled' => true
			),
			'properties' => array(
				'woeid' => array(
					'type' => 'long'
				),
				'woeid_continent' => array(
					'type' => 'long'
				),
				'woeid_country' => array(
					'type' => 'long'
				),
				'woeid_county' => array(
					'type' => 'long'
				),
				'woeid_localadmin' => array(
					'type' => 'long'
				),
				'woeid_state' => array(
					'type' => 'long'
				)
			)
		),
		self::PLACETYPES_TYPE => array(
			'_timestamp' => array(
				'enabled' => true
			)
		)
	)
);

?>
