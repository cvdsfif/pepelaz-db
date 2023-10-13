module.exports = {
    preset: "ts-jest",
    roots: ['<rootDir>/tests'],
    testEnvironment: "node",
    testMatch: ['**/*.test.ts'],
    transform: {
        '^.+\\.tsx?$': 'ts-jest'
    },
    verbose: true,
    collectCoverage: true,
    collectCoverageFrom: ['!integration-test/*', '!tests/*', '!dev-test/*']
};