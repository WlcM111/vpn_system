package tg_bot_gateway

import "fmt"

// referralScreenText формирует экран реферальной программы.
func referralScreenText(stats *referralStatsView, botUsername string) string {
	link := referralLink(botUsername, stats.Code)

	remainder := 0
	if stats.UsersPerMonth > 0 {
		remainder = stats.UsersPerMonth - (stats.InvitedTotal % stats.UsersPerMonth)
		if remainder == stats.UsersPerMonth {
			remainder = 0
		}
	}

	var sb []byte
	add := func(s string) { sb = append(sb, s...) }

	add("🎁 *Реферальная программа*\n\n")
	if stats.UsersPerMonth == 1 {
		add("За *каждого друга*, который оформит платную подписку, вы получаете *1 месяц бесплатно*.\n\n")
	} else {
		add(fmt.Sprintf("За каждые *%d* %s, оформивших платную подписку, вы получаете *1 месяц бесплатно*.\n\n",
			stats.UsersPerMonth, pluralFriends(stats.UsersPerMonth)))
	}
	add(fmt.Sprintf("👥 Приглашено (с оплатой): *%d*\n", stats.InvitedTotal))
	add(fmt.Sprintf("🎟 Доступно бесплатных месяцев: *%d*\n", stats.AvailableMonths))
	if stats.AvailableMonths > 0 {
		add("\n✅ Есть доступные месяцы! Нажмите *«Получить бесплатные месяцы»*.\n")
	} else if remainder > 0 {
		add(fmt.Sprintf("\nЕщё *%d* %s — и получите следующий бесплатный месяц.\n",
			remainder, pluralFriends(remainder)))
	}
	add("\n🔗 Ваша ссылка для приглашений:\n")
	add("`" + link + "`\n")
	add("\nПоделитесь ей с друзьями — как только они оформят платную подписку, приглашение засчитается.")

	return string(sb)
}

// pluralFriends возвращает правильную форму слова «друг» для числа n:
// 1, 21 → «друг»; 2-4, 22-24 → «друга»; 0, 5-20, 25-30 → «друзей».
// Нужна, чтобы текст оставался грамотным при любом REFERRAL_USERS_PER_MONTH.
func pluralFriends(n int) string {
	if n < 0 {
		n = -n
	}
	if mod100 := n % 100; mod100 >= 11 && mod100 <= 14 {
		return "друзей"
	}
	switch n % 10 {
	case 1:
		return "друг"
	case 2, 3, 4:
		return "друга"
	default:
		return "друзей"
	}
}
