'use strict'

const path = require('path')

const GridFSStore = require('../lib/stores/GridFSStore')

const shared = require('./Test-Stores.shared')
const assert = require('assert')

describe.only('GridFSDataStore', function () {
  before(function() {
    this.testFileSize = 960244
    this.testFileName = 'test.mp4'
    this.storePath = '/test/output'
    this.testFilePath = path.resolve(__dirname, 'fixtures', this.testFileName)
  })

  beforeEach(function() {
    this.datastore = new GridFSStore({})
  })

  shared.shouldHaveStoreMethods()
  shared.shouldCreateUploads()
  shared.shouldRemoveUploads()
  shared.shouldWriteUploads()
  shared.shouldHandleOffset()
})
