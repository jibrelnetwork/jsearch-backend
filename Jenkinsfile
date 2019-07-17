builder(
        jUnitReportsPath: 'junit-reports',
        coverageReportsPath: 'coverage-reports',
        buildTasks: [
                [
                        name: "Linters",
                        type: "lint",
                        method: "inside",
                        runAsUser: "root",
                        entrypoint: "",
                        jUnitPath: '/junit-reports',
                        command: [
                                'pip install --no-cache-dir -r requirements-test.txt',
                                'mkdir -p /junit-reports',
                                'flake8 -v --format junit-xml --output-file=/junit-reports/flake8-junit-report.xml',
                                // 'mypy --junit-xml=/junit-reports/mypy-junit-report.xml .',
                        ],
                ],
                [
                        name: 'Tests',
                        type: 'test',
                        method: 'inside',
                        runAsUser: 'root',
                        entrypoint: '',
                        jUnitPath: '/junit-reports',
                        coveragePath: '/coverage-reports',
                        environment: [
                                JSEARCH_MAIN_DB_TEST: 'postgres://app:pass@maindb/jsearch-maindb',
                                JSEARCH_RAW_DB_TEST: 'postgres://app:pass@rawdb/jsearch-rawdb',
                        ],
                        sidecars: [
                                maindb: [
                                        image: 'postgres:11.0-alpine',
                                        environment: [
                                                POSTGRES_USER: 'app',
                                                POSTGRES_PASSWORD: 'pass',
                                                POSTGRES_DB: 'jsearch-maindb',
                                        ],
                                ],
                                rawdb: [
                                        image: 'postgres:11.0-alpine',
                                        environment: [
                                                POSTGRES_USER: 'app',
                                                POSTGRES_PASSWORD: 'pass',
                                                POSTGRES_DB: 'jsearch-rawdb',
                                        ],
                                ],
                        ],
                        command: [
                                'pip install --no-cache-dir -r requirements-test.txt',
                                'mkdir -p /junit-reports',
                                'pytest --junitxml=/junit-reports/pytest-junit-report.xml --cov-report xml:/coverage-reports/pytest-coverage-report.xml',
                        ],
                ]
        ],
)
