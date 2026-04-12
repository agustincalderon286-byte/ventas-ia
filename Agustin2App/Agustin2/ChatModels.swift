import Foundation

struct CRMProfile: Decodable {
  let displayName: String
  let skin: String
  let themeLabel: String

  init(displayName: String = "", skin: String = "", themeLabel: String = "") {
    self.displayName = displayName
    self.skin = skin
    self.themeLabel = themeLabel
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    displayName = container.decodeString("displayName")
    skin = container.decodeString("skin")
    themeLabel = container.decodeString("themeLabel")
  }
}

struct CRMMeResponse: Decodable {
  let authenticated: Bool
  let configured: Bool
  let email: String
  let allowedEmail: String
  let profile: CRMProfile

  init(
    authenticated: Bool = false,
    configured: Bool = true,
    email: String = "",
    allowedEmail: String = "",
    profile: CRMProfile = CRMProfile()
  ) {
    self.authenticated = authenticated
    self.configured = configured
    self.email = email
    self.allowedEmail = allowedEmail
    self.profile = profile
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    authenticated = container.decodeBool("authenticated")
    configured = container.decodeBool("configured", defaultValue: true)
    email = container.decodeString("email")
    allowedEmail = container.decodeString("allowedEmail")
    profile = container.decodeModel(CRMProfile.self, forKey: "profile", defaultValue: CRMProfile())
  }
}

struct CRMLoginResponse: Decodable {
  let ok: Bool
  let email: String
  let profile: CRMProfile

  init(ok: Bool = false, email: String = "", profile: CRMProfile = CRMProfile()) {
    self.ok = ok
    self.email = email
    self.profile = profile
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    ok = container.decodeBool("ok")
    email = container.decodeString("email")
    profile = container.decodeModel(CRMProfile.self, forKey: "profile", defaultValue: CRMProfile())
  }
}

struct CRMStatusOption: Decodable, Hashable, Identifiable {
  let value: String
  let label: String

  var id: String { value }

  init(value: String = "", label: String = "") {
    self.value = value
    self.label = label
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    value = container.decodeString("value")
    label = container.decodeString("label")
  }
}

struct CRMLeadTracking: Decodable, Hashable {
  let gclid: String
  let gbraid: String
  let wbraid: String
  let utmSource: String
  let utmMedium: String
  let utmCampaign: String
  let utmTerm: String
  let utmContent: String
  let landingPath: String
  let landingUrl: String
  let referrer: String

  init(
    gclid: String = "",
    gbraid: String = "",
    wbraid: String = "",
    utmSource: String = "",
    utmMedium: String = "",
    utmCampaign: String = "",
    utmTerm: String = "",
    utmContent: String = "",
    landingPath: String = "",
    landingUrl: String = "",
    referrer: String = ""
  ) {
    self.gclid = gclid
    self.gbraid = gbraid
    self.wbraid = wbraid
    self.utmSource = utmSource
    self.utmMedium = utmMedium
    self.utmCampaign = utmCampaign
    self.utmTerm = utmTerm
    self.utmContent = utmContent
    self.landingPath = landingPath
    self.landingUrl = landingUrl
    self.referrer = referrer
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    gclid = container.decodeString("gclid")
    gbraid = container.decodeString("gbraid")
    wbraid = container.decodeString("wbraid")
    utmSource = container.decodeString("utmSource")
    utmMedium = container.decodeString("utmMedium")
    utmCampaign = container.decodeString("utmCampaign")
    utmTerm = container.decodeString("utmTerm")
    utmContent = container.decodeString("utmContent")
    landingPath = container.decodeString("landingPath")
    landingUrl = container.decodeString("landingUrl")
    referrer = container.decodeString("referrer")
  }

  var isPaidTraffic: Bool {
    !gclid.isEmpty ||
      !gbraid.isEmpty ||
      !wbraid.isEmpty ||
      utmMedium.localizedCaseInsensitiveContains("cpc") ||
      utmMedium.localizedCaseInsensitiveContains("paid") ||
      utmMedium.localizedCaseInsensitiveContains("ppc") ||
      utmSource.localizedCaseInsensitiveContains("google")
  }

  var sourceSummary: String {
    if isPaidTraffic {
      return "Google Ads"
    }

    if !utmSource.isEmpty {
      return utmSource.replacingOccurrences(of: "_", with: " ").capitalized
    }

    return ""
  }

  var campaignSummary: String {
    if !utmCampaign.isEmpty {
      return utmCampaign.replacingOccurrences(of: "_", with: " ")
    }

    if !utmTerm.isEmpty {
      return utmTerm.replacingOccurrences(of: "_", with: " ")
    }

    return ""
  }

  var landingSummary: String {
    if !landingPath.isEmpty {
      return landingPath
    }

    if !landingUrl.isEmpty {
      return landingUrl
    }

    return ""
  }
}

struct CRMConversationEntry: Decodable, Hashable, Identifiable {
  let role: String
  let content: String
  let createdAt: String

  var id: String {
    "\(role)-\(createdAt)-\(String(content.prefix(24)))"
  }

  init(role: String = "", content: String = "", createdAt: String = "") {
    self.role = role
    self.content = content
    self.createdAt = createdAt
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    role = container.decodeString("role")
    content = container.decodeString("content")
    createdAt = container.decodeString("createdAt")
  }
}

struct CRMLead: Decodable, Identifiable, Hashable {
  let id: String
  let fullName: String
  let phone: String
  let phoneDisplay: String
  let email: String
  let projectType: String
  let location: String
  let addressLine: String
  let zipCode: String
  let city: String
  let propertyType: String
  let projectSize: String
  let timeline: String
  let ownershipStatus: String
  let budgetRange: String
  let urgency: String
  let bestContactWindow: String
  let preferredLanguage: String
  let qualificationTier: String
  let qualificationNotes: String
  let sourceProspectorName: String
  let sourceProspectorEmail: String
  let details: String
  let photoFileNames: [String]
  let status: String
  let statusLabel: String
  let nextAction: String
  let nextActionAt: String
  let bestContactDay: String
  let bestContactTime: String
  let callbackIntent: String
  let callbackRequestedAt: String
  let callbackAlertedAt: String
  let privateNotes: String
  let lastUserMessage: String
  let lastAssistantMessage: String
  let conversationHistory: [CRMConversationEntry]
  let estimateTitle: String
  let estimateScope: String
  let estimateAmount: Double
  let clientDocumentType: String
  let clientDocumentDescription: String
  let clientDocumentWorkDate: String
  let clientDocumentWarranty: String
  let pageTitle: String
  let pagePath: String
  let pageUrl: String
  let referrer: String
  let sourceType: String
  let tracking: CRMLeadTracking
  let createdAt: String
  let updatedAt: String
  let lastContactAt: String

  init(
    id: String = "",
    fullName: String = "",
    phone: String = "",
    phoneDisplay: String = "",
    email: String = "",
    projectType: String = "",
    location: String = "",
    addressLine: String = "",
    zipCode: String = "",
    city: String = "",
    propertyType: String = "",
    projectSize: String = "",
    timeline: String = "",
    ownershipStatus: String = "",
    budgetRange: String = "",
    urgency: String = "",
    bestContactWindow: String = "",
    preferredLanguage: String = "",
    qualificationTier: String = "",
    qualificationNotes: String = "",
    sourceProspectorName: String = "",
    sourceProspectorEmail: String = "",
    details: String = "",
    photoFileNames: [String] = [],
    status: String = "new",
    statusLabel: String = "",
    nextAction: String = "",
    nextActionAt: String = "",
    bestContactDay: String = "",
    bestContactTime: String = "",
    callbackIntent: String = "",
    callbackRequestedAt: String = "",
    callbackAlertedAt: String = "",
    privateNotes: String = "",
    lastUserMessage: String = "",
    lastAssistantMessage: String = "",
    conversationHistory: [CRMConversationEntry] = [],
    estimateTitle: String = "",
    estimateScope: String = "",
    estimateAmount: Double = 0,
    clientDocumentType: String = "",
    clientDocumentDescription: String = "",
    clientDocumentWorkDate: String = "",
    clientDocumentWarranty: String = "",
    pageTitle: String = "",
    pagePath: String = "",
    pageUrl: String = "",
    referrer: String = "",
    sourceType: String = "website_form",
    tracking: CRMLeadTracking = CRMLeadTracking(),
    createdAt: String = "",
    updatedAt: String = "",
    lastContactAt: String = ""
  ) {
    self.id = id
    self.fullName = fullName
    self.phone = phone
    self.phoneDisplay = phoneDisplay
    self.email = email
    self.projectType = projectType
    self.location = location
    self.addressLine = addressLine
    self.zipCode = zipCode
    self.city = city
    self.propertyType = propertyType
    self.projectSize = projectSize
    self.timeline = timeline
    self.ownershipStatus = ownershipStatus
    self.budgetRange = budgetRange
    self.urgency = urgency
    self.bestContactWindow = bestContactWindow
    self.preferredLanguage = preferredLanguage
    self.qualificationTier = qualificationTier
    self.qualificationNotes = qualificationNotes
    self.sourceProspectorName = sourceProspectorName
    self.sourceProspectorEmail = sourceProspectorEmail
    self.details = details
    self.photoFileNames = photoFileNames
    self.status = status
    self.statusLabel = statusLabel
    self.nextAction = nextAction
    self.nextActionAt = nextActionAt
    self.bestContactDay = bestContactDay
    self.bestContactTime = bestContactTime
    self.callbackIntent = callbackIntent
    self.callbackRequestedAt = callbackRequestedAt
    self.callbackAlertedAt = callbackAlertedAt
    self.privateNotes = privateNotes
    self.lastUserMessage = lastUserMessage
    self.lastAssistantMessage = lastAssistantMessage
    self.conversationHistory = conversationHistory
    self.estimateTitle = estimateTitle
    self.estimateScope = estimateScope
    self.estimateAmount = estimateAmount
    self.clientDocumentType = clientDocumentType
    self.clientDocumentDescription = clientDocumentDescription
    self.clientDocumentWorkDate = clientDocumentWorkDate
    self.clientDocumentWarranty = clientDocumentWarranty
    self.pageTitle = pageTitle
    self.pagePath = pagePath
    self.pageUrl = pageUrl
    self.referrer = referrer
    self.sourceType = sourceType
    self.tracking = tracking
    self.createdAt = createdAt
    self.updatedAt = updatedAt
    self.lastContactAt = lastContactAt
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    id = container.decodeString("id")
    fullName = container.decodeString("fullName")
    phone = container.decodeString("phone")
    phoneDisplay = container.decodeString("phoneDisplay")
    email = container.decodeString("email")
    projectType = container.decodeString("projectType")
    location = container.decodeString("location")
    addressLine = container.decodeString("addressLine")
    zipCode = container.decodeString("zipCode")
    city = container.decodeString("city")
    propertyType = container.decodeString("propertyType")
    projectSize = container.decodeString("projectSize")
    timeline = container.decodeString("timeline")
    ownershipStatus = container.decodeString("ownershipStatus")
    budgetRange = container.decodeString("budgetRange")
    urgency = container.decodeString("urgency")
    bestContactWindow = container.decodeString("bestContactWindow")
    preferredLanguage = container.decodeString("preferredLanguage")
    qualificationTier = container.decodeString("qualificationTier")
    qualificationNotes = container.decodeString("qualificationNotes")
    sourceProspectorName = container.decodeString("sourceProspectorName")
    sourceProspectorEmail = container.decodeString("sourceProspectorEmail")
    details = container.decodeString("details")
    photoFileNames = container.decodeStringArray("photoFileNames")
    status = container.decodeString("status", defaultValue: "new")
    statusLabel = container.decodeString("statusLabel")
    nextAction = container.decodeString("nextAction")
    nextActionAt = container.decodeString("nextActionAt")
    bestContactDay = container.decodeString("bestContactDay")
    bestContactTime = container.decodeString("bestContactTime")
    callbackIntent = container.decodeString("callbackIntent")
    callbackRequestedAt = container.decodeString("callbackRequestedAt")
    callbackAlertedAt = container.decodeString("callbackAlertedAt")
    privateNotes = container.decodeString("privateNotes")
    lastUserMessage = container.decodeString("lastUserMessage")
    lastAssistantMessage = container.decodeString("lastAssistantMessage")
    conversationHistory = container.decodeArray(CRMConversationEntry.self, forKey: "conversationHistory")
    estimateTitle = container.decodeString("estimateTitle")
    estimateScope = container.decodeString("estimateScope")
    estimateAmount = container.decodeDouble("estimateAmount")
    clientDocumentType = container.decodeString("clientDocumentType")
    clientDocumentDescription = container.decodeString("clientDocumentDescription")
    clientDocumentWorkDate = container.decodeString("clientDocumentWorkDate")
    clientDocumentWarranty = container.decodeString("clientDocumentWarranty")
    pageTitle = container.decodeString("pageTitle")
    pagePath = container.decodeString("pagePath")
    pageUrl = container.decodeString("pageUrl")
    referrer = container.decodeString("referrer")
    sourceType = container.decodeString("sourceType", defaultValue: "website_form")
    tracking = container.decodeModel(CRMLeadTracking.self, forKey: "tracking", defaultValue: CRMLeadTracking())
    createdAt = container.decodeString("createdAt")
    updatedAt = container.decodeString("updatedAt")
    lastContactAt = container.decodeString("lastContactAt")
  }

  var phoneDigits: String {
    normalizePhoneDigits(phoneDisplay.isEmpty ? phone : phoneDisplay)
  }

  var sanitizedFullName: String {
    let cleaned = fullName.trimmingCharacters(in: .whitespacesAndNewlines)
    let normalized = cleaned.lowercased()

    if cleaned.isEmpty || normalized == "website chat lead" || normalized == "website lead" || normalized == "lead" {
      return ""
    }

    return cleaned
  }

  var hasContactInfo: Bool {
    !phoneDigits.isEmpty || !email.isEmpty
  }

  var hasFollowUpDate: Bool {
    !nextActionAt.isEmpty
  }

  var hasScheduledWorkDate: Bool {
    !clientDocumentWorkDate.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
  }

  var displayName: String {
    if !sanitizedFullName.isEmpty {
      return sanitizedFullName
    }

    if !phoneDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return phoneDisplay
    }

    if !email.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return email
    }

    return "Lead pending info"
  }

  var addressSummary: String {
    let parts = [addressLine, city, zipCode]
      .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty }
    return parts.joined(separator: ", ")
  }

  var prospectorCaptureSummary: String {
    let name = sourceProspectorName.trimmingCharacters(in: .whitespacesAndNewlines)
    let tier = qualificationTier.trimmingCharacters(in: .whitespacesAndNewlines)

    if !name.isEmpty && !tier.isEmpty {
      return "Captured by \(name) · Tier \(tier)"
    }

    if !name.isEmpty {
      return "Captured by \(name)"
    }

    if !tier.isEmpty {
      return "Tier \(tier)"
    }

    return ""
  }

  var hasFieldIntakeDetails: Bool {
    ![
      addressLine,
      zipCode,
      city,
      propertyType,
      projectSize,
      timeline,
      ownershipStatus,
      budgetRange,
      urgency,
      bestContactWindow,
      preferredLanguage,
      qualificationTier,
      qualificationNotes,
      sourceProspectorName,
      sourceProspectorEmail,
    ]
    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
    .filter { !$0.isEmpty }
    .isEmpty
  }

  var missingFieldLabels: [String] {
    var labels: [String] = []

    if sanitizedFullName.isEmpty {
      labels.append("name")
    }

    if phoneDigits.isEmpty {
      labels.append("phone")
    }

    if email.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      labels.append("email")
    }

    if projectType.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      labels.append("service")
    }

    if location.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      labels.append("location")
    }

    if details.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      labels.append("details")
    }

    return labels
  }

  var missingFieldSummary: String {
    let labels = missingFieldLabels

    guard !labels.isEmpty else {
      return ""
    }

    if labels.count <= 2 {
      return labels.joined(separator: ", ")
    }

    return "\(labels[0]), \(labels[1]) +\(labels.count - 2) more"
  }

  var isProfileIncomplete: Bool {
    !missingFieldLabels.isEmpty
  }
}

struct CRMApplicant: Decodable, Identifiable, Hashable {
  let id: String
  let fullName: String
  let phone: String
  let phoneDisplay: String
  let email: String
  let positionApplied: String
  let roleTrack: String
  let languages: String
  let yearsExperience: String
  let experienceSummary: String
  let hasTools: String
  let hasTransportation: String
  let fieldReady: String
  let location: String
  let bestInterviewDay: String
  let bestInterviewTime: String
  let status: String
  let statusLabel: String
  let nextAction: String
  let nextActionAt: String
  let interviewRequestedAt: String
  let alertSentAt: String
  let privateNotes: String
  let detailsSummary: String
  let sourceType: String
  let sourceChannel: String
  let sourceLabel: String
  let pageTitle: String
  let pagePath: String
  let pageUrl: String
  let referrer: String
  let tracking: CRMLeadTracking
  let lastUserMessage: String
  let lastAssistantMessage: String
  let conversationHistory: [CRMConversationEntry]
  let createdAt: String
  let updatedAt: String
  let lastContactAt: String

  init(
    id: String = "",
    fullName: String = "",
    phone: String = "",
    phoneDisplay: String = "",
    email: String = "",
    positionApplied: String = "",
    roleTrack: String = "",
    languages: String = "",
    yearsExperience: String = "",
    experienceSummary: String = "",
    hasTools: String = "",
    hasTransportation: String = "",
    fieldReady: String = "",
    location: String = "",
    bestInterviewDay: String = "",
    bestInterviewTime: String = "",
    status: String = "new",
    statusLabel: String = "",
    nextAction: String = "",
    nextActionAt: String = "",
    interviewRequestedAt: String = "",
    alertSentAt: String = "",
    privateNotes: String = "",
    detailsSummary: String = "",
    sourceType: String = "assistant_chat_job",
    sourceChannel: String = "",
    sourceLabel: String = "",
    pageTitle: String = "",
    pagePath: String = "",
    pageUrl: String = "",
    referrer: String = "",
    tracking: CRMLeadTracking = CRMLeadTracking(),
    lastUserMessage: String = "",
    lastAssistantMessage: String = "",
    conversationHistory: [CRMConversationEntry] = [],
    createdAt: String = "",
    updatedAt: String = "",
    lastContactAt: String = ""
  ) {
    self.id = id
    self.fullName = fullName
    self.phone = phone
    self.phoneDisplay = phoneDisplay
    self.email = email
    self.positionApplied = positionApplied
    self.roleTrack = roleTrack
    self.languages = languages
    self.yearsExperience = yearsExperience
    self.experienceSummary = experienceSummary
    self.hasTools = hasTools
    self.hasTransportation = hasTransportation
    self.fieldReady = fieldReady
    self.location = location
    self.bestInterviewDay = bestInterviewDay
    self.bestInterviewTime = bestInterviewTime
    self.status = status
    self.statusLabel = statusLabel
    self.nextAction = nextAction
    self.nextActionAt = nextActionAt
    self.interviewRequestedAt = interviewRequestedAt
    self.alertSentAt = alertSentAt
    self.privateNotes = privateNotes
    self.detailsSummary = detailsSummary
    self.sourceType = sourceType
    self.sourceChannel = sourceChannel
    self.sourceLabel = sourceLabel
    self.pageTitle = pageTitle
    self.pagePath = pagePath
    self.pageUrl = pageUrl
    self.referrer = referrer
    self.tracking = tracking
    self.lastUserMessage = lastUserMessage
    self.lastAssistantMessage = lastAssistantMessage
    self.conversationHistory = conversationHistory
    self.createdAt = createdAt
    self.updatedAt = updatedAt
    self.lastContactAt = lastContactAt
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    id = container.decodeString("id")
    fullName = container.decodeString("fullName")
    phone = container.decodeString("phone")
    phoneDisplay = container.decodeString("phoneDisplay")
    email = container.decodeString("email")
    positionApplied = container.decodeString("positionApplied")
    roleTrack = container.decodeString("roleTrack")
    languages = container.decodeString("languages")
    yearsExperience = container.decodeString("yearsExperience")
    experienceSummary = container.decodeString("experienceSummary")
    hasTools = container.decodeString("hasTools")
    hasTransportation = container.decodeString("hasTransportation")
    fieldReady = container.decodeString("fieldReady")
    location = container.decodeString("location")
    bestInterviewDay = container.decodeString("bestInterviewDay")
    bestInterviewTime = container.decodeString("bestInterviewTime")
    status = container.decodeString("status", defaultValue: "new")
    statusLabel = container.decodeString("statusLabel")
    nextAction = container.decodeString("nextAction")
    nextActionAt = container.decodeString("nextActionAt")
    interviewRequestedAt = container.decodeString("interviewRequestedAt")
    alertSentAt = container.decodeString("alertSentAt")
    privateNotes = container.decodeString("privateNotes")
    detailsSummary = container.decodeString("detailsSummary")
    sourceType = container.decodeString("sourceType", defaultValue: "assistant_chat_job")
    sourceChannel = container.decodeString("sourceChannel")
    sourceLabel = container.decodeString("sourceLabel")
    pageTitle = container.decodeString("pageTitle")
    pagePath = container.decodeString("pagePath")
    pageUrl = container.decodeString("pageUrl")
    referrer = container.decodeString("referrer")
    tracking = container.decodeModel(CRMLeadTracking.self, forKey: "tracking", defaultValue: CRMLeadTracking())
    lastUserMessage = container.decodeString("lastUserMessage")
    lastAssistantMessage = container.decodeString("lastAssistantMessage")
    conversationHistory = container.decodeArray(CRMConversationEntry.self, forKey: "conversationHistory")
    createdAt = container.decodeString("createdAt")
    updatedAt = container.decodeString("updatedAt")
    lastContactAt = container.decodeString("lastContactAt")
  }

  var phoneDigits: String {
    normalizePhoneDigits(phoneDisplay.isEmpty ? phone : phoneDisplay)
  }

  var sanitizedFullName: String {
    let cleaned = fullName.trimmingCharacters(in: .whitespacesAndNewlines)
    let normalized = cleaned.lowercased()

    if cleaned.isEmpty ||
      normalized == "website chat lead" ||
      normalized == "website lead" ||
      normalized == "lead pending info" ||
      normalized == "job applicant" ||
      normalized == "candidate" {
      return ""
    }

    return cleaned
  }

  var displayName: String {
    if !sanitizedFullName.isEmpty {
      return sanitizedFullName
    }

    if !phoneDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return phoneDisplay
    }

    if !email.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      return email
    }

    return "Applicant pending info"
  }

  var roleLabel: String {
    let explicitRole = positionApplied.trimmingCharacters(in: .whitespacesAndNewlines)
    if !explicitRole.isEmpty {
      return explicitRole
    }

    let track = roleTrack.trimmingCharacters(in: .whitespacesAndNewlines)
    if !track.isEmpty {
      return track.replacingOccurrences(of: "_", with: " ").capitalized
    }

    return "Role pending"
  }

  var manualPrivateNotes: String {
    let marker = "[Agustin Applicant Notes]"
    let source = privateNotes

    guard let range = source.range(of: marker) else {
      return source.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    return String(source[..<range.lowerBound]).trimmingCharacters(in: .whitespacesAndNewlines)
  }

  var profileHighlights: [String] {
    [
      languages.trimmingCharacters(in: .whitespacesAndNewlines),
      yearsExperience.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "" : "\(yearsExperience) yrs",
      location.trimmingCharacters(in: .whitespacesAndNewlines),
    ].filter { !$0.isEmpty }
  }
}

struct CRMLeadActivity: Decodable, Identifiable, Hashable {
  let id: String
  let leadId: String
  let applicantId: String
  let activityType: String
  let title: String
  let body: String
  let pagePath: String
  let pageUrl: String
  let createdAt: String

  init(
    id: String = "",
    leadId: String = "",
    applicantId: String = "",
    activityType: String = "",
    title: String = "",
    body: String = "",
    pagePath: String = "",
    pageUrl: String = "",
    createdAt: String = ""
  ) {
    self.id = id
    self.leadId = leadId
    self.applicantId = applicantId
    self.activityType = activityType
    self.title = title
    self.body = body
    self.pagePath = pagePath
    self.pageUrl = pageUrl
    self.createdAt = createdAt
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    id = container.decodeString("id")
    leadId = container.decodeString("leadId")
    applicantId = container.decodeString("applicantId")
    activityType = container.decodeString("activityType")
    title = container.decodeString("title")
    body = container.decodeString("body")
    pagePath = container.decodeString("pagePath")
    pageUrl = container.decodeString("pageUrl")
    createdAt = container.decodeString("createdAt")
  }
}

struct CRMLeadAsset: Decodable, Identifiable, Hashable {
  let id: String
  let leadId: String
  let fileName: String
  let mimeType: String
  let sizeBytes: Int
  let uploadedAt: String
  let downloadUrl: String

  init(
    id: String = "",
    leadId: String = "",
    fileName: String = "",
    mimeType: String = "",
    sizeBytes: Int = 0,
    uploadedAt: String = "",
    downloadUrl: String = ""
  ) {
    self.id = id
    self.leadId = leadId
    self.fileName = fileName
    self.mimeType = mimeType
    self.sizeBytes = sizeBytes
    self.uploadedAt = uploadedAt
    self.downloadUrl = downloadUrl
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    id = container.decodeString("id")
    leadId = container.decodeString("leadId")
    fileName = container.decodeString("fileName")
    mimeType = container.decodeString("mimeType")
    sizeBytes = container.decodeInt("sizeBytes")
    uploadedAt = container.decodeString("uploadedAt")
    downloadUrl = container.decodeString("downloadUrl")
  }
}

struct CRMDashboardSummary: Decodable {
  let totalLeads: Int
  let newLeads: Int
  let contactedLeads: Int
  let quotedLeads: Int
  let bookedLeads: Int
  let wonLeads: Int
  let lostLeads: Int
  let archivedLeads: Int
  let totalApplicants: Int
  let newApplicants: Int
  let interviewApplicants: Int
  let phoneClicks30d: Int
  let emailClicks30d: Int
  let quoteSubmits30d: Int

  init(
    totalLeads: Int = 0,
    newLeads: Int = 0,
    contactedLeads: Int = 0,
    quotedLeads: Int = 0,
    bookedLeads: Int = 0,
    wonLeads: Int = 0,
    lostLeads: Int = 0,
    archivedLeads: Int = 0,
    totalApplicants: Int = 0,
    newApplicants: Int = 0,
    interviewApplicants: Int = 0,
    phoneClicks30d: Int = 0,
    emailClicks30d: Int = 0,
    quoteSubmits30d: Int = 0
  ) {
    self.totalLeads = totalLeads
    self.newLeads = newLeads
    self.contactedLeads = contactedLeads
    self.quotedLeads = quotedLeads
    self.bookedLeads = bookedLeads
    self.wonLeads = wonLeads
    self.lostLeads = lostLeads
    self.archivedLeads = archivedLeads
    self.totalApplicants = totalApplicants
    self.newApplicants = newApplicants
    self.interviewApplicants = interviewApplicants
    self.phoneClicks30d = phoneClicks30d
    self.emailClicks30d = emailClicks30d
    self.quoteSubmits30d = quoteSubmits30d
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    totalLeads = container.decodeInt("totalLeads")
    newLeads = container.decodeInt("newLeads")
    contactedLeads = container.decodeInt("contactedLeads")
    quotedLeads = container.decodeInt("quotedLeads")
    bookedLeads = container.decodeInt("bookedLeads")
    wonLeads = container.decodeInt("wonLeads")
    lostLeads = container.decodeInt("lostLeads")
    archivedLeads = container.decodeInt("archivedLeads")
    totalApplicants = container.decodeInt("totalApplicants")
    newApplicants = container.decodeInt("newApplicants")
    interviewApplicants = container.decodeInt("interviewApplicants")
    phoneClicks30d = container.decodeInt("phoneClicks30d")
    emailClicks30d = container.decodeInt("emailClicks30d")
    quoteSubmits30d = container.decodeInt("quoteSubmits30d")
  }
}

struct CRMDashboardFilters: Decodable {
  let status: String
  let search: String
  let projectType: String

  init(status: String = "", search: String = "", projectType: String = "") {
    self.status = status
    self.search = search
    self.projectType = projectType
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    status = container.decodeString("status")
    search = container.decodeString("search")
    projectType = container.decodeString("projectType")
  }
}

struct CRMServiceBreakdownItem: Decodable, Hashable, Identifiable {
  let label: String
  let count: Int

  var id: String { label }

  init(label: String = "", count: Int = 0) {
    self.label = label
    self.count = count
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    label = container.decodeString("label")
    count = container.decodeInt("count")
  }
}

struct CRMDashboardResponse: Decodable {
  let summary: CRMDashboardSummary
  let filters: CRMDashboardFilters
  let serviceBreakdown: [CRMServiceBreakdownItem]
  let leads: [CRMLead]
  let recentActivity: [CRMLeadActivity]
  let statusOptions: [CRMStatusOption]
  let recentApplicants: [CRMApplicant]

  init(
    summary: CRMDashboardSummary = CRMDashboardSummary(),
    filters: CRMDashboardFilters = CRMDashboardFilters(),
    serviceBreakdown: [CRMServiceBreakdownItem] = [],
    leads: [CRMLead] = [],
    recentActivity: [CRMLeadActivity] = [],
    statusOptions: [CRMStatusOption] = [],
    recentApplicants: [CRMApplicant] = []
  ) {
    self.summary = summary
    self.filters = filters
    self.serviceBreakdown = serviceBreakdown
    self.leads = leads
    self.recentActivity = recentActivity
    self.statusOptions = statusOptions
    self.recentApplicants = recentApplicants
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    summary = container.decodeModel(CRMDashboardSummary.self, forKey: "summary", defaultValue: CRMDashboardSummary())
    filters = container.decodeModel(CRMDashboardFilters.self, forKey: "filters", defaultValue: CRMDashboardFilters())
    serviceBreakdown = container.decodeArray(CRMServiceBreakdownItem.self, forKey: "serviceBreakdown")
    leads = container.decodeArray(CRMLead.self, forKey: "leads")
    recentActivity = container.decodeArray(CRMLeadActivity.self, forKey: "recentActivity")
    statusOptions = container.decodeArray(CRMStatusOption.self, forKey: "statusOptions")
    recentApplicants = container.decodeArray(CRMApplicant.self, forKey: "recentApplicants")
  }
}

struct CRMApplicantsResponse: Decodable {
  let applicants: [CRMApplicant]

  init(applicants: [CRMApplicant] = []) {
    self.applicants = applicants
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    applicants = container.decodeArray(CRMApplicant.self, forKey: "applicants")
  }
}

struct CRMApplicantDetailResponse: Decodable {
  let applicant: CRMApplicant
  let activity: [CRMLeadActivity]

  init(applicant: CRMApplicant = CRMApplicant(), activity: [CRMLeadActivity] = []) {
    self.applicant = applicant
    self.activity = activity
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    applicant = container.decodeModel(CRMApplicant.self, forKey: "applicant", defaultValue: CRMApplicant())
    activity = container.decodeArray(CRMLeadActivity.self, forKey: "activity")
  }
}

struct CRMLeadDetailResponse: Decodable {
  let lead: CRMLead
  let assets: [CRMLeadAsset]
  let activity: [CRMLeadActivity]

  init(
    lead: CRMLead = CRMLead(),
    assets: [CRMLeadAsset] = [],
    activity: [CRMLeadActivity] = []
  ) {
    self.lead = lead
    self.assets = assets
    self.activity = activity
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    lead = container.decodeModel(CRMLead.self, forKey: "lead", defaultValue: CRMLead())
    assets = container.decodeArray(CRMLeadAsset.self, forKey: "assets")
    activity = container.decodeArray(CRMLeadActivity.self, forKey: "activity")
  }
}

struct CRMPushDevice: Decodable {
  let id: String
  let deviceName: String
  let bundleId: String
  let appEnvironment: String
  let notificationsEnabled: Bool
  let isActive: Bool
  let lastSeenAt: String
  let lastPushAt: String
  let lastPushError: String

  init(
    id: String = "",
    deviceName: String = "",
    bundleId: String = "",
    appEnvironment: String = "",
    notificationsEnabled: Bool = false,
    isActive: Bool = false,
    lastSeenAt: String = "",
    lastPushAt: String = "",
    lastPushError: String = ""
  ) {
    self.id = id
    self.deviceName = deviceName
    self.bundleId = bundleId
    self.appEnvironment = appEnvironment
    self.notificationsEnabled = notificationsEnabled
    self.isActive = isActive
    self.lastSeenAt = lastSeenAt
    self.lastPushAt = lastPushAt
    self.lastPushError = lastPushError
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    id = container.decodeString("id")
    deviceName = container.decodeString("deviceName")
    bundleId = container.decodeString("bundleId")
    appEnvironment = container.decodeString("appEnvironment")
    notificationsEnabled = container.decodeBool("notificationsEnabled")
    isActive = container.decodeBool("isActive")
    lastSeenAt = container.decodeString("lastSeenAt")
    lastPushAt = container.decodeString("lastPushAt")
    lastPushError = container.decodeString("lastPushError")
  }
}

struct CRMPushRegistrationResponse: Decodable {
  let ok: Bool
  let apnsConfigured: Bool
  let message: String
  let device: CRMPushDevice

  init(
    ok: Bool = false,
    apnsConfigured: Bool = false,
    message: String = "",
    device: CRMPushDevice = CRMPushDevice()
  ) {
    self.ok = ok
    self.apnsConfigured = apnsConfigured
    self.message = message
    self.device = device
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    ok = container.decodeBool("ok")
    apnsConfigured = container.decodeBool("apnsConfigured")
    message = container.decodeString("message")
    device = container.decodeModel(CRMPushDevice.self, forKey: "device", defaultValue: CRMPushDevice())
  }
}

struct CRMPushTestResponse: Decodable {
  let ok: Bool
  let apnsConfigured: Bool
  let delivered: Bool
  let deliveredCount: Int
  let deviceCount: Int
  let message: String

  init(
    ok: Bool = false,
    apnsConfigured: Bool = false,
    delivered: Bool = false,
    deliveredCount: Int = 0,
    deviceCount: Int = 0,
    message: String = ""
  ) {
    self.ok = ok
    self.apnsConfigured = apnsConfigured
    self.delivered = delivered
    self.deliveredCount = deliveredCount
    self.deviceCount = deviceCount
    self.message = message
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: JSONCodingKey.self)
    ok = container.decodeBool("ok")
    apnsConfigured = container.decodeBool("apnsConfigured")
    delivered = container.decodeBool("delivered")
    deliveredCount = container.decodeInt("deliveredCount")
    deviceCount = container.decodeInt("deviceCount")
    message = container.decodeString("message")
  }
}

struct CRMPushRegistrationPayload: Encodable {
  let deviceToken: String
  let bundleId: String
  let appEnvironment: String
  let deviceName: String
  let appVersion: String
  let buildNumber: String
  let authorizationStatus: String
  let notificationsEnabled: Bool

  var registrationFingerprint: String {
    [
      deviceToken,
      bundleId,
      appEnvironment,
      deviceName,
      appVersion,
      buildNumber,
      authorizationStatus,
      notificationsEnabled ? "1" : "0",
    ].joined(separator: "|")
  }
}

struct CRMLeadUpdatePayload: Encodable {
  let fullName: String
  let phoneDisplay: String
  let email: String
  let projectType: String
  let location: String
  let details: String
  let bestContactDay: String
  let bestContactTime: String
  let clientDocumentWorkDate: String
  let status: String
  let nextAction: String
  let nextActionAt: String
  let privateNotes: String
  let note: String
}

struct CRMApplicantUpdatePayload: Encodable {
  let fullName: String
  let phoneDisplay: String
  let email: String
  let positionApplied: String
  let languages: String
  let yearsExperience: String
  let experienceSummary: String
  let hasTools: String
  let hasTransportation: String
  let fieldReady: String
  let location: String
  let bestInterviewDay: String
  let bestInterviewTime: String
  let status: String
  let nextAction: String
  let nextActionAt: String
  let privateNotes: String
  let note: String
}

private struct JSONCodingKey: CodingKey {
  let stringValue: String
  let intValue: Int?

  init?(stringValue: String) {
    self.stringValue = stringValue
    intValue = nil
  }

  init?(intValue: Int) {
    stringValue = String(intValue)
    self.intValue = intValue
  }
}

private extension KeyedDecodingContainer where Key == JSONCodingKey {
  func decodeString(_ key: String, defaultValue: String = "") -> String {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return defaultValue
    }

    return (try? decodeIfPresent(String.self, forKey: codingKey)) ?? defaultValue
  }

  func decodeBool(_ key: String, defaultValue: Bool = false) -> Bool {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return defaultValue
    }

    return (try? decodeIfPresent(Bool.self, forKey: codingKey)) ?? defaultValue
  }

  func decodeInt(_ key: String, defaultValue: Int = 0) -> Int {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return defaultValue
    }

    return (try? decodeIfPresent(Int.self, forKey: codingKey))
      ?? Int((try? decodeIfPresent(String.self, forKey: codingKey)) ?? "")
      ?? defaultValue
  }

  func decodeDouble(_ key: String, defaultValue: Double = 0) -> Double {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return defaultValue
    }

    return (try? decodeIfPresent(Double.self, forKey: codingKey))
      ?? Double((try? decodeIfPresent(String.self, forKey: codingKey)) ?? "")
      ?? defaultValue
  }

  func decodeArray<T: Decodable>(_ type: T.Type, forKey key: String) -> [T] {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return []
    }

    return (try? decodeIfPresent([T].self, forKey: codingKey)) ?? []
  }

  func decodeStringArray(_ key: String) -> [String] {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return []
    }

    return (try? decodeIfPresent([String].self, forKey: codingKey)) ?? []
  }

  func decodeModel<T: Decodable>(_ type: T.Type, forKey key: String, defaultValue: T) -> T {
    guard let codingKey = JSONCodingKey(stringValue: key) else {
      return defaultValue
    }

    return (try? decodeIfPresent(T.self, forKey: codingKey)) ?? defaultValue
  }
}

private func normalizePhoneDigits(_ value: String) -> String {
  let digits = value.replacingOccurrences(of: "\\D", with: "", options: .regularExpression)

  if digits.count == 11, digits.hasPrefix("1") {
    return String(digits.dropFirst())
  }

  if digits.count >= 10 {
    return String(digits.prefix(10))
  }

  return digits
}
